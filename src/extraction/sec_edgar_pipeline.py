# finlens/extraction/sec_edgar_pipeline.py
import requests
import zipfile
import json
import logging
import os
import time
from pathlib import Path
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
import re
import collections
import csv
from io import StringIO
import argparse  # For command-line arguments
import concurrent.futures  # For ThreadPoolExecutor
import multiprocessing  # For Pool
from threading import Lock  # For thread-safe rate limiter
import sys  # For sys.path modification
import unicodedata  # For filename cleaning

# --- Database & ORM Imports ---
from sqlalchemy import select, join, insert, or_  # Added or_ for NULL check
from sqlalchemy.orm import Session
from sqlalchemy.dialects.mysql import insert as mysql_insert
from bs4 import BeautifulSoup  # For HTML parsing in helper

# --- Local Module Import ---
import src.database as db

# --- Load Environment Variables ---
load_dotenv()

# --- Basic Logging Setup ---
log_file_path = "sec_pipeline.log"
logging.basicConfig(
    level=logging.INFO,  # Default level, overridden by args
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file_path, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ],
    force=True)
logger = logging.getLogger('sec_pipeline')
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
logger.info(f"Logging initialised. Log file: {log_file_path}")


# --- Helper function for bulk ingestion worker processes ---
def parse_cik_json(file_path):
    """Parses a single CIK JSON file for selected company and filing data."""
    cik_match = re.match(r'CIK(\d{10})', file_path.name)
    if not cik_match: return None
    cik = cik_match.group(1)
    company_data = None
    filings_data = []
    parse_errors = 0
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # --- Extract Requested Company Info ---
        company_name = data.get('entityName', '') or data.get(
            'name', '') or f"Company CIK {cik}"
        addresses = data.get('addresses', {})
        business_address = addresses.get('business', {}) if isinstance(
            addresses, dict) else {}

        # Convert 0/1 to Boolean or None
        owner_exists = data.get('insiderTransactionForOwnerExists')
        issuer_exists = data.get('insiderTransactionForIssuerExists')

        company_data = {
            'cik':
            cik,
            'name':
            company_name.strip(),
            'sic':
            data.get('sic'),  # Keeping SIC for filtering
            'entity_type':
            data.get('entityType'),
            'sic_description':
            data.get('sicDescription'),
            'insider_tx_for_owner_exists':
            bool(owner_exists) if owner_exists in [0, 1] else None,
            'insider_tx_for_issuer_exists':
            bool(issuer_exists) if issuer_exists in [0, 1] else None,
            'phone':
            data.get('phone'),
            # Business Address components for querying
            'business_street1':
            business_address.get('street1'),
            'business_street2':
            business_address.get('street2'),
            'business_city':
            business_address.get('city'),
            'business_state_or_country':
            business_address.get('stateOrCountry'),
            'business_state_or_country_desc':
            business_address.get('stateOrCountryDescription'),
            'business_zip':
            business_address.get('zipCode'),
        }
        # --- End Company Info Extraction ---

        # --- Filings Extraction (Unchanged) ---
        if 'filings' in data and 'recent' in data['filings']:
            # ... (same logic as before to append to filings_data) ...
            recent = data['filings']['recent']
            keys = ['form', 'filingDate', 'accessionNumber', 'primaryDocument']
            if all(k in recent and isinstance(recent[k], list) for k in keys):
                if recent.get('accessionNumber'):
                    min_len = min(
                        len(recent[k]) for k in keys if recent.get(k))
                else:
                    min_len = 0
                if min_len > 0:
                    forms, dates, acc_nums, docs = recent['form'], recent[
                        'filingDate'], recent['accessionNumber'], recent[
                            'primaryDocument']
                    for i in range(min_len):
                        try:
                            acc_num = acc_nums[i]
                            filename = docs[i]
                            form = forms[i]
                            date_str = dates[i]
                            if not all([acc_num, filename, form, date_str]):
                                parse_errors += 1
                                continue
                            try:
                                filing_date_obj = datetime.strptime(
                                    date_str, '%Y-%m-%d').date()
                            except ValueError:
                                parse_errors += 1
                                continue
                            filings_data.append({
                                "cik":
                                cik,
                                "form_type":
                                form,
                                "filing_date":
                                filing_date_obj,
                                "accession_number":
                                acc_num,
                                "primary_document_filename":
                                filename
                            })
                        except Exception:
                            parse_errors += 1
                            continue
    except Exception:
        parse_errors += 1
    return company_data, filings_data, parse_errors, file_path


# --- SECDataPipeline Class ---
class SECDataPipeline:

    def __init__(self, session_factory, base_dir_path: Path):
        if session_factory is None:
            raise ValueError("Session factory cannot be None.")
        self.session_factory = session_factory
        self.base_dir = base_dir_path
        self.submissions_dir = self.base_dir / 'submissions'
        self.tenk_docs_dir = self.base_dir / '10k_documents'
        self.submissions_bulk_url = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
        self.submissions_api_base = "https://data.sec.gov/submissions/CIK"
        self.edgar_archive_base = "https://www.sec.gov/Archives/edgar/data"

        self.submissions_dir.mkdir(parents=True, exist_ok=True)
        self.tenk_docs_dir.mkdir(parents=True, exist_ok=True)
        logger.info(
            f"Data directories ensured: {self.submissions_dir}, {self.tenk_docs_dir}"
        )

        user_agent = os.getenv('SEC_USER_AGENT')
        if not user_agent:
            logger.critical(
                "FATAL: SEC_USER_AGENT environment variable not set.")
            sys.exit(1)
        logger.info(f"Using SEC User-Agent: {user_agent}")
        self.headers = {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"
        }
        self.api_headers = {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "data.sec.gov"
        }
        self.last_request_time = 0
        self._rate_limit_lock = Lock()

    def _respect_rate_limit(self, delay=0.11):
        with self._rate_limit_lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            target_delay = max(delay, 0.11)
            if time_since_last_request < target_delay:
                time.sleep(target_delay - time_since_last_request)
            self.last_request_time = time.time()

    def download_bulk_file(self, url, output_path):
        self._respect_rate_limit()
        logger.info(f"Downloading {url} to {output_path}")
        try:
            response = requests.get(url,
                                    headers=self.headers,
                                    stream=True,
                                    timeout=600)
            response.raise_for_status()
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=65536):
                    f.write(chunk)  # Larger chunk
            logger.info(f"Successfully downloaded {url}")
            return True
        except requests.exceptions.Timeout:
            logger.error(f"Timeout download {url}")
            return False
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error {url}: {e.response.status_code}")
            return False
        except Exception as e:
            logger.error(f"Download error {url}: {e}", exc_info=True)
            return False

    def extract_zip(self, zip_path, extract_dir):
        logger.info(f"Extracting {zip_path} to {extract_dir}")
        if not zip_path.exists():
            logger.error(f"ZIP file not found: {zip_path}")
            return False
        try:
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(extract_dir)
            logger.info(f"Successfully extracted {zip_path}")
            return True
        except zipfile.BadZipFile:
            logger.error(f"Bad ZIP file: {zip_path}")
            return False
        except Exception as e:
            logger.error(f"Error extracting {zip_path}: {e}", exc_info=True)
            return False

    # --- Parallelized BULK Ingestion Method ---
    def ingest_bulk_submissions_files_to_db(self, num_workers):
        """
        Ingests data from bulk JSON files using multiprocessing and handles
        company inserts/updates and filing inserts correctly.
        """
        logger.info(
            f"Starting parallel bulk ingestion from: {self.submissions_dir} using {num_workers} workers"
        )
        db_session: Session = self.session_factory()
        all_cik_files = list(self.submissions_dir.rglob("CIK*.json"))
        if not all_cik_files:
            logger.warning(f"No CIK JSON files in {self.submissions_dir}.")
            self.session_factory.remove()
            return False
        logger.info(f"Found {len(all_cik_files)} CIK JSON files to process.")

        # Use a thread-safe dictionary for collecting company data if needed,
        # but processing results in main thread is usually fine.
        companies_to_add_or_update = {}  # Use CIK as key
        filings_to_potentially_add = []
        total_parse_errors = 0
        files_processed = 0

        try:
            # --- Parallel Parsing ---
            with multiprocessing.Pool(processes=num_workers) as pool:
                chunksize = max(1, len(all_cik_files) // (num_workers * 5))
                logger.info(
                    f"Processing files in chunks of approx {chunksize}...")
                results_iterator = pool.imap_unordered(parse_cik_json,
                                                       all_cik_files,
                                                       chunksize=chunksize)
                for result in results_iterator:
                    files_processed += 1
                    if files_processed % 10000 == 0:
                        logger.info(
                            f"Collected results from {files_processed}/{len(all_cik_files)} files..."
                        )
                    if result is None:
                        total_parse_errors += 1
                        continue
                    company_data, filings_data, parse_errors, file_path = result
                    total_parse_errors += parse_errors
                    # Store latest parsed data for each CIK (overwrites if CIK seen again)
                    if company_data:
                        companies_to_add_or_update[
                            company_data['cik']] = company_data
                    if filings_data:
                        filings_to_potentially_add.extend(filings_data)
                    if parse_errors > 0:
                        logger.debug(
                            f"{parse_errors} parse errors in file: {file_path.name}"
                        )

            logger.info(
                f"Parsing complete. Processed: {files_processed}, Total errors: {total_parse_errors}"
            )
            logger.info(
                f"Collected {len(companies_to_add_or_update)} unique companies, {len(filings_to_potentially_add)} potential filings."
            )

            # --- Separate New vs Existing Companies ---
            all_ciks_from_json = list(companies_to_add_or_update.keys())
            existing_ciks_in_db = set()
            new_company_mappings = []
            existing_company_mappings = []

            if all_ciks_from_json:
                logger.info("Checking existing companies in database...")
                # Check DB in batches
                batch_size_check = 10000
                for i in range(0, len(all_ciks_from_json), batch_size_check):
                    batch_ciks = all_ciks_from_json[i:i + batch_size_check]
                    if not batch_ciks: continue
                    try:
                        stmt = select(db.Company.cik).where(
                            db.Company.cik.in_(batch_ciks))
                        results = db_session.execute(stmt)
                        existing_ciks_in_db.update(row.cik for row in results)
                    except Exception as check_err:
                        logger.error(
                            f"Error checking company existence batch: {check_err}",
                            exc_info=True)
                        # Handle error - maybe assume all in batch are new? Or fail? For now, log and continue.

                # Split the data
                for cik, data in companies_to_add_or_update.items():
                    if cik in existing_ciks_in_db:
                        existing_company_mappings.append(data)
                    else:
                        new_company_mappings.append(data)

                logger.info(
                    f"Found {len(new_company_mappings)} new companies and {len(existing_company_mappings)} existing companies to potentially update."
                )

            companies_to_add_or_update.clear()
            all_ciks_from_json.clear()
            existing_ciks_in_db.clear()  # Free memory

            # --- Bulk Insert New Companies ---
            if new_company_mappings:
                logger.info(
                    f"Bulk inserting {len(new_company_mappings)} new companies..."
                )
                batch_size = 10000
                for i in range(0, len(new_company_mappings), batch_size):
                    batch = new_company_mappings[i:i + batch_size]
                    num = (i // batch_size) + 1
                    logger.info(
                        f"Processing new company insert batch {num}...")
                    try:
                        db_session.bulk_insert_mappings(db.Company, batch)
                        db_session.commit()
                        logger.info(
                            f"Processed new company insert batch {num}.")
                    except Exception as e:
                        logger.error(
                            f"New company insert error batch {num}: {e}",
                            exc_info=True)
                        db_session.rollback()
            else:
                logger.info("No new companies to insert.")
            new_company_mappings.clear()

            # --- Bulk Update Existing Companies ---
            if existing_company_mappings:
                logger.info(
                    f"Bulk updating {len(existing_company_mappings)} existing companies..."
                )
                batch_size = 10000
                for i in range(0, len(existing_company_mappings), batch_size):
                    batch = existing_company_mappings[i:i + batch_size]
                    num = (i // batch_size) + 1
                    logger.info(
                        f"Processing existing company update batch {num}...")
                    try:
                        # Ensure primary key ('cik') is present in each dict in batch
                        db_session.bulk_update_mappings(db.Company, batch)
                        db_session.commit()
                        logger.info(
                            f"Processed existing company update batch {num}.")
                    except Exception as e:
                        logger.error(
                            f"Existing company update error batch {num}: {e}",
                            exc_info=True)
                        db_session.rollback()
            else:
                logger.info(
                    "No existing companies need updates based on parsed data.")
            existing_company_mappings.clear()

            # --- Insert Filings (Using INSERT IGNORE - Unchanged) ---
            logger.info("Filtering duplicates from aggregated filings list...")
            unique_filings_dict = {
                f['accession_number']: f
                for f in filings_to_potentially_add
            }
            filings_to_potentially_add.clear()
            final_filings_to_insert = list(unique_filings_dict.values())
            unique_filings_dict.clear()
            logger.info(
                f"Inserting {len(final_filings_to_insert)} unique filings...")
            if final_filings_to_insert:
                batch_size = 20000
                for i in range(0, len(final_filings_to_insert), batch_size):
                    batch = final_filings_to_insert[i:i + batch_size]
                    num = (i // batch_size) + 1
                    logger.info(f"Processing filing insert batch {num}...")
                    try:
                        stmt = mysql_insert(
                            db.Filing).values(batch).prefix_with(
                                "IGNORE", dialect="mysql")
                        result = db_session.execute(stmt)
                        db_session.commit()
                        logger.info(
                            f"Processed insert batch {num}. Rows affected: {result.rowcount}"
                        )
                    except Exception as e:
                        logger.error(f"Filing insert error batch {num}: {e}",
                                     exc_info=True)
                        db_session.rollback()
            else:
                logger.warning("No unique filings found to insert.")
            final_filings_to_insert.clear()

            logger.info("Parallel bulk data ingestion finished.")
            return True
        except Exception as e:
            logger.error(f"Major error during parallel bulk ingest: {e}",
                         exc_info=True)
            db_session.rollback()
            return False
        finally:
            self.session_factory.remove()

    def download_and_ingest_bulk(self, num_workers):
        """Downloads, extracts, and ingests the bulk submissions file."""
        logger.info(f"Starting full bulk process at {datetime.now()}")
        submissions_zip = self.base_dir / "submissions.zip"
        if self.download_bulk_file(self.submissions_bulk_url, submissions_zip):
            if self.extract_zip(submissions_zip, self.submissions_dir):
                self.ingest_bulk_submissions_files_to_db(num_workers)
            else:
                logger.error("Submissions bulk extraction failed.")
        else:
            logger.error("Failed to download submissions bulk file.")
        logger.info(f"Full bulk process completed at {datetime.now()}")

    # --- Daily Index Incremental Update ---
    def incremental_update(self):
        """
        Updates database using SEC Daily Index files to find new filings.
        Fetches full details via API for newly discovered companies.
        """
        logger.info(
            f"Starting incremental update using Daily Index files at {datetime.now()}"
        )
        db_session: Session = self.session_factory(
        )  # Ensure self.session_factory is accessible

        try:
            # --- 1. Determine which dates to check ---
            dates_to_check = []
            today = datetime.now().date()
            for i in range(
                    31):  # Check today and previous 30 days for robustness
                dates_to_check.append(today - timedelta(days=i))

            filings_from_index = {}  # Store as {accession_num: {details}}

            # --- 2. Download and Parse Daily Index Files ---
            logger.info(
                f"Checking daily indices for dates: {sorted(list(set(dates_to_check)))}"
            )
            for check_date in sorted(list(set(
                    dates_to_check))):  # Ensure unique dates, chronological
                year = check_date.year
                quarter = (check_date.month - 1) // 3 + 1
                date_str_url = check_date.strftime('%Y%m%d')
                index_url = f"https://www.sec.gov/Archives/edgar/daily-index/{year}/QTR{quarter}/master.{date_str_url}.idx"
                logger.debug(f"Fetching daily index: {index_url}"
                             )  # Use debug for less noise

                try:
                    self._respect_rate_limit(
                    )  # Ensure this method exists and works
                    response = requests.get(
                        index_url, headers=self.headers,
                        timeout=60)  # Ensure self.headers exists

                    # Handle Not Found / Forbidden Gracefully
                    if response.status_code in [404, 403]:
                        logger.info(
                            f"Index not found/forbidden for {check_date} (status {response.status_code})."
                        )
                        continue  # Skip this date

                    response.raise_for_status(
                    )  # Raise error for other bad statuses (e.g., 5xx)

                    # --- 3. Parse the content ---
                    content = response.text
                    data_start_pos = content.find('-----------')
                    if data_start_pos == -1:
                        logger.warning(
                            f"No data marker in index {check_date}.")
                        continue
                    data_content = content[data_start_pos +
                                           len('-----------'):].strip()
                    if not data_content:
                        logger.info(
                            f"No data after header in index {check_date}.")
                        continue

                    data_io = StringIO(data_content)
                    lines_processed_in_file = 0
                    for line_num, line in enumerate(
                            data_io):  # Add line number for context
                        line = line.strip()
                        if not line: continue
                        parts = line.split('|')
                        if len(parts) < 5:
                            logger.warning(
                                f"Skipping malformed line {line_num+11} idx {check_date}"
                            )
                            continue  # Adjust line number based on header skip
                        lines_processed_in_file += 1

                        try:
                            cik_raw, name_raw, form_type_raw, date_filed_str_raw, filename_raw = parts[
                                0], parts[1], parts[2], parts[3], parts[4]

                            # Aggressive Cleaning and Targeted Parsing for Date
                            digits_only = "".join(
                                filter(str.isdigit, date_filed_str_raw))
                            filing_date_obj = None
                            if len(digits_only) == 8:
                                try:
                                    filing_date_obj = datetime.strptime(
                                        digits_only, '%Y%m%d').date()
                                except ValueError:
                                    logger.warning(
                                        f"Failed YYYYMMDD parse '{digits_only}' raw={repr(date_filed_str_raw)}. Skip: {line}"
                                    )
                                    continue
                            else:
                                date_filed_str_stripped = date_filed_str_raw.strip(
                                )
                                try:
                                    filing_date_obj = datetime.strptime(
                                        date_filed_str_stripped,
                                        '%Y-%m-%d').date()
                                except ValueError:
                                    logger.warning(
                                        f"Unable date parse: Raw={repr(date_filed_str_raw)}, Digits='{digits_only}'. Skip: {line}"
                                    )
                                    continue

                            # Accession number extraction
                            cik = cik_raw.strip()
                            form_type = form_type_raw.strip()
                            name = name_raw.strip()
                            filename = filename_raw.strip()
                            accession_num = None
                            acc_match = re.search(r'(\d{10}-\d{2}-\d{6})',
                                                  filename)
                            if acc_match: accession_num = acc_match.group(1)
                            else:
                                acc_match_nodash = re.search(
                                    r'(\d{18,20})', filename)
                                if acc_match_nodash:
                                    acc_no_dash = acc_match_nodash.group(1)
                                    if len(acc_no_dash) == 18:
                                        accession_num = f"{acc_no_dash[:10]}-{acc_no_dash[10:12]}-{acc_no_dash[12:]}"
                                    elif len(acc_no_dash) == 20:
                                        accession_num = f"{acc_no_dash[:10]}-{acc_no_dash[10:12]}-{acc_no_dash[12:18]}"
                                    else:
                                        logger.debug(
                                            f"Cannot format no-dash acc num {acc_no_dash}"
                                        )
                                        continue
                                else:
                                    logger.debug(
                                        f"Cannot extract acc num from {filename}"
                                    )
                                    continue

                            primary_doc = Path(filename).name
                            accession_num_cleaned = accession_num.strip()
                            # Store minimal info needed initially
                            filings_from_index[accession_num_cleaned] = {
                                "cik": cik,
                                "form_type": form_type,
                                "filing_date": filing_date_obj,
                                "accession_number": accession_num_cleaned,
                                "primary_document_filename":
                                primary_doc.strip(),
                                "company_name": name
                            }
                        except Exception as parse_err:
                            logger.error(
                                f"General error parsing line {line_num+11} idx {check_date}: '{line}' -> {parse_err}",
                                exc_info=False)

                    logger.info(
                        f"Parsed {lines_processed_in_file} lines from index {check_date}."
                    )
                except requests.exceptions.RequestException as e:
                    logger.error(
                        f"Request error fetch index {check_date}: {e}")
                except Exception as e:
                    logger.error(f"Error processing index {check_date}: {e}",
                                 exc_info=True)

            if not filings_from_index:
                logger.info("No valid filings parsed from recent indices.")
                self.session_factory.remove()
                return

            # --- 4. Check Database for Existing Accession Numbers ---
            all_index_acc_nums = list(filings_from_index.keys())
            logger.info(
                f"Checking {len(all_index_acc_nums)} unique accession numbers against database."
            )
            existing_db_acc_nums = set()
            batch_size_check = 10000
            for i in range(0, len(all_index_acc_nums), batch_size_check):
                batch_acc_nums = all_index_acc_nums[i:i + batch_size_check]
                if not batch_acc_nums: continue
                try:
                    stmt = select(db.Filing.accession_number).where(
                        db.Filing.accession_number.in_(batch_acc_nums))
                    results = db_session.execute(stmt)
                    existing_db_acc_nums.update(row.accession_number
                                                for row in results)
                except Exception as check_err:
                    logger.error(
                        f"Error checking accession batch: {check_err}",
                        exc_info=True)

            # --- 5. Determine Missing Filings ---
            new_filing_acc_nums = set(
                all_index_acc_nums) - existing_db_acc_nums
            logger.info(
                f"Found {len(new_filing_acc_nums)} new filings to add.")

            # --- 6. Prepare New Filings for Insert ---
            filings_to_insert = []
            new_ciks_data = {}  # Stores {cik: name} from index
            for acc_num in new_filing_acc_nums:
                filing_details = filings_from_index[acc_num]
                company_name = filing_details.pop(
                    "company_name", None)  # Remove temp name field
                filings_to_insert.append(filing_details)
                # Store the name found in the index for potential new company insert
                if filing_details['cik'] not in new_ciks_data:
                    new_ciks_data[filing_details[
                        'cik']] = company_name or f"Company CIK {filing_details['cik']}"

            # --- 7. Insert New Filings (Bulk) ---
            if filings_to_insert:
                logger.info(
                    f"Inserting {len(filings_to_insert)} new filings...")
                batch_size_insert = 10000
                for i in range(0, len(filings_to_insert), batch_size_insert):
                    batch = filings_to_insert[i:i + batch_size_insert]
                    num = (i // batch_size_insert) + 1
                    logger.info(f"Processing new filing insert batch {num}...")
                    try:
                        stmt = mysql_insert(
                            db.Filing).values(batch).prefix_with(
                                "IGNORE", dialect="mysql")
                        result = db_session.execute(stmt)
                        db_session.commit()
                        logger.info(
                            f"Processed filing insert batch {num}. Rows affected: {result.rowcount}"
                        )
                    except Exception as e:
                        logger.error(
                            f"Error inserting filing batch {num}: {e}",
                            exc_info=True)
                        db_session.rollback()
            else:
                logger.info("No new filings needed insertion.")

            # --- 8. Add/Update Newly Discovered Companies ---
            if new_ciks_data:
                all_new_ciks_list = list(new_ciks_data.keys())
                logger.info(
                    f"Checking {len(all_new_ciks_list)} CIKs potentially needing company data update..."
                )
                existing_ciks_in_db = set()
                comp_check_batch = 10000
                for i in range(0, len(all_new_ciks_list), comp_check_batch):
                    batch_ciks = all_new_ciks_list[i:i + comp_check_batch]
                    if not batch_ciks: continue
                    try:
                        stmt_comp_check = select(db.Company.cik).where(
                            db.Company.cik.in_(batch_ciks))
                        results = db_session.execute(stmt_comp_check)
                        existing_ciks_in_db.update(row.cik for row in results)
                    except Exception as comp_check_err:
                        logger.error(
                            f"Error checking company batch: {comp_check_err}",
                            exc_info=True)

                ciks_to_add_or_update = list(
                    set(all_new_ciks_list
                        ))  # All CIKs from new filings might need update
                ciks_genuinely_new = list(
                    set(ciks_to_add_or_update) - existing_ciks_in_db)
                if ciks_genuinely_new:
                    logger.info(
                        f"Found {len(ciks_genuinely_new)} genuinely new CIKs.")
                else:
                    logger.info(
                        "No genuinely new CIKs found, but will check/update details for involved CIKs."
                    )

                if ciks_to_add_or_update:
                    companies_to_upsert = []
                    logger.info(
                        f"Fetching details via API for {len(ciks_to_add_or_update)} companies..."
                    )
                    processed_api_ciks = 0
                    for cik in ciks_to_add_or_update:
                        processed_api_ciks += 1
                        if processed_api_ciks % 50 == 0:
                            logger.info(
                                f"API lookup progress: {processed_api_ciks}/{len(ciks_to_add_or_update)}"
                            )
                        # Initialize with CIK and default name from index data
                        company_details = {
                            'cik': cik,
                            'name': new_ciks_data.get(
                                cik, f"Company CIK {cik}"
                            ),  # Use name from index as default
                            'sic': None,
                            'entity_type': None,
                            'sic_description': None,
                            'insider_tx_for_owner_exists': None,
                            'insider_tx_for_issuer_exists': None,
                            'phone': None,
                            'business_street1': None,
                            'business_street2': None,
                            'business_city': None,
                            'business_state_or_country': None,
                            'business_state_or_country_desc': None,
                            'business_zip': None
                        }
                        try:
                            self._respect_rate_limit()
                            api_url = f"{self.submissions_api_base}{cik.zfill(10)}.json"
                            api_resp = requests.get(api_url,
                                                    headers=self.api_headers,
                                                    timeout=20)
                            if api_resp.status_code == 200:
                                api_data = api_resp.json()
                                # --- Populate details from API response ---
                                company_details['name'] = api_data.get(
                                    'entityName', '') or api_data.get(
                                        'name', company_details['name']
                                    )  # Prefer entityName
                                company_details['sic'] = api_data.get('sic')
                                company_details['entity_type'] = api_data.get(
                                    'entityType')
                                company_details[
                                    'sic_description'] = api_data.get(
                                        'sicDescription')
                                owner_exists = api_data.get(
                                    'insiderTransactionForOwnerExists')
                                issuer_exists = api_data.get(
                                    'insiderTransactionForIssuerExists')
                                company_details[
                                    'insider_tx_for_owner_exists'] = bool(
                                        owner_exists) if owner_exists in [
                                            0, 1
                                        ] else None
                                company_details[
                                    'insider_tx_for_issuer_exists'] = bool(
                                        issuer_exists) if issuer_exists in [
                                            0, 1
                                        ] else None
                                company_details['phone'] = api_data.get(
                                    'phone')
                                addresses = api_data.get('addresses', {})
                                business_address = addresses.get(
                                    'business', {}) if isinstance(
                                        addresses, dict) else {}
                                company_details[
                                    'business_street1'] = business_address.get(
                                        'street1')
                                company_details[
                                    'business_street2'] = business_address.get(
                                        'street2')
                                company_details[
                                    'business_city'] = business_address.get(
                                        'city')
                                company_details[
                                    'business_state_or_country'] = business_address.get(
                                        'stateOrCountry')
                                company_details[
                                    'business_state_or_country_desc'] = business_address.get(
                                        'stateOrCountryDescription')
                                company_details[
                                    'business_zip'] = business_address.get(
                                        'zipCode')
                            else:
                                logger.warning(
                                    f"API lookup failed ({api_resp.status_code}) for CIK {cik}"
                                )
                        except Exception as api_err:
                            logger.warning(
                                f"API lookup error for CIK {cik}: {api_err}")
                        companies_to_upsert.append(company_details)

                    # Use bulk_merge_mappings for insert/update
                    comp_batch_size = 5000
                    logger.info(
                        f"Merging details for {len(companies_to_upsert)} companies..."
                    )
                    for i in range(0, len(companies_to_upsert),
                                   comp_batch_size):
                        comp_batch = companies_to_upsert[i:i + comp_batch_size]
                        comp_num = (i // comp_batch_size) + 1
                        logger.info(
                            f"Processing company merge batch {comp_num}...")
                        try:
                            db_session.bulk_merge_mappings(
                                db.Company, comp_batch)
                            db_session.commit()
                            logger.info(
                                f"Processed company merge batch {comp_num}.")
                        except Exception as e:
                            logger.error(
                                f"Error merging company batch {comp_num}: {e}",
                                exc_info=True)
                            db_session.rollback()
                else:
                    logger.info(
                        "No new CIKs found to add/update company details.")

            logger.info(
                f"Incremental update via Daily Index finished at {datetime.now()}"
            )
        except Exception as e:
            logger.error(f"Major error during Daily Index update: {e}",
                         exc_info=True)
            db_session.rollback()  # Rollback on major error
        finally:
            self.session_factory.remove()

    # --- Helper to Find Primary HTM Document ---
    def _find_primary_htm_document(
            self,
            cik: str,
            accession_number: str,
            target_form_type: str = '10-K') -> tuple[str | None, bool]:
        """
        Fetches filing index page, finds primary HTM filename, and checks for ABS indicators.
        Returns: (filename_or_None, is_likely_abs_flag)
        """
        logger.debug(
            f"Finding primary HTM for {cik}/{accession_number} (target: {target_form_type})"
        )
        acc_no_dashes = accession_number.replace('-', '')
        try:
            cik_int_str = str(int(cik))
        except ValueError:
            logger.error(f"Invalid CIK: {cik}")
            return None, False
        index_page_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int_str}/{acc_no_dashes}/{accession_number}-index.html"
        logger.debug(f"Fetching index page: {index_page_url}")

        try:
            self._respect_rate_limit()
            response = requests.get(index_page_url,
                                    headers=self.headers,
                                    timeout=30)
            if response.status_code != 200:
                logger.warning(
                    f"Index page HTTP {response.status_code} for {accession_number}"
                )
                return None, False
            soup = BeautifulSoup(response.content, 'lxml')
            doc_table = soup.find('table', class_='tableFile')
            if not doc_table:
                header = soup.find(['h2', 'div'],
                                   string=re.compile(r'Document Format Files',
                                                     re.IGNORECASE))
                doc_table = header.find_next_sibling(
                    'table') if header else None
            if not doc_table:
                doc_table = soup.find('table',
                                      summary=re.compile(
                                          "Document Format Files",
                                          re.IGNORECASE))
            if not doc_table:
                logger.warning(f"Cannot find document table: {index_page_url}")
                return None, False

            rows = doc_table.find_all('tr')
            if len(rows) < 2:
                logger.warning(f"No data rows in doc table: {index_page_url}")
                return None, False

            target_form_variations = {
                target_form_type, target_form_type + '/A'
            }
            abs_exhibit_prefixes = ('EX-33', 'EX-34', 'EX-35', 'EX-1122',
                                    'EX-1123')
            parsed_htm_filename = None
            is_likely_abs = False

            for i, row in enumerate(rows[1:]):
                cells = row.find_all('td')
                if len(cells) >= 4:
                    try:
                        doc_type = cells[3].get_text(strip=True)
                        # Check for ABS first
                        if any(
                                doc_type.startswith(prefix)
                                for prefix in abs_exhibit_prefixes):
                            is_likely_abs = True
                            logger.debug(
                                f"Detected likely ABS {accession_number} due to Exhibit: {doc_type}"
                            )
                            break  # Found ABS indicator, no need to check further rows

                        # If not ABS and haven't found HTM yet, check this row
                        if not parsed_htm_filename:
                            link_tag = cells[2].find('a', href=True)
                            doc_filename = link_tag.get_text(
                                strip=True) if link_tag else cells[2].get_text(
                                    strip=True)
                            if doc_type in target_form_variations and doc_filename.lower(
                            ).endswith(('.htm', '.html')):
                                parsed_htm_filename = doc_filename
                                logger.debug(
                                    f"Found candidate HTM '{parsed_htm_filename}' type '{doc_type}'"
                                )
                                # Don't break yet, keep checking subsequent rows for ABS indicators
                    except Exception as cell_err:
                        logger.warning(
                            f"Error parsing cell row {i+1} on {index_page_url}: {cell_err}"
                        )

            # Return results after checking all rows
            if is_likely_abs:
                return None, True  # Return None for filename if ABS
            elif parsed_htm_filename:
                return parsed_htm_filename, False  # Return found HTM filename
            else:
                logger.debug(
                    f"No matching primary HTM doc found for {target_form_type} on index page: {index_page_url}"
                )
                return None, False  # No HTM found, not detected as ABS

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error fetching index {index_page_url}: {e}")
            return None, False
        except Exception as e:
            logger.error(f"Error parsing index {index_page_url}: {e}",
                         exc_info=True)
            return None, False

    # --- Parallelized Document Download Worker ---
    def download_filing_document_worker(self, task):
        """Worker function to download a single document."""
        url, output_path = task
        try:
            success = self.download_filing_document(url, output_path)
            return url, success, None
        except Exception as e:
            logger.error(f"Worker error {url}: {e}", exc_info=False)
            return url, False, e

    # --- Query and Download 10Ks (Uses SIC Filter + HTM Helper) ---
    def query_filter_and_download_10k_filings(self,
                                              year_start,
                                              year_end,
                                              download_docs,
                                              num_threads,
                                              max_downloads=None):
        """Queries DB for 10-Ks (excluding ABS/Funds via SIC), finds primary HTM, optionally downloads."""
        if year_end is None: year_end = datetime.now().date().year
        logger.info(
            f"Querying DB for 10-K filings ({year_start}-{year_end}), excluding non-operating SICs."
        )
        db_session: Session = self.session_factory()
        filings_to_download = []
        found_filings_metadata = []
        abs_fund_sics = ['6189', '6722', '6726']  # SICs to exclude

        try:
            start_date = date(year_start, 1, 1)
            end_date = date(year_end, 12, 31)
            stmt = select(db.Company.name, db.Filing.cik, db.Filing.filing_date, db.Filing.accession_number, db.Filing.primary_document_filename)\
                   .join(db.Company, db.Filing.cik == db.Company.cik)\
                   .where(db.Filing.form_type == '10-K')\
                   .where(db.Filing.filing_date.between(start_date, end_date))\
                   .where(or_(db.Company.sic == None, db.Company.sic.notin_(abs_fund_sics)))\
                   .order_by(db.Filing.filing_date.desc())

            results_cursor = db_session.execute(
                stmt.execution_options(yield_per=5000))
            logger.info(
                "Processing query results & checking SEC index pages...")
            processed_db = 0
            skipped_no_target_filename = 0
            found_htm = 0
            used_db_filename = 0
            index_fetch_errors = 0
            abs_filings_skipped_index = 0

            for row in results_cursor.mappings():
                processed_db += 1
                if processed_db % 1000 == 0:
                    logger.info(
                        f"Processed {processed_db} potential operating company 10-Ks from DB..."
                    )

                cik = row['cik']
                accession_num = row['accession_number']
                db_filename = row['primary_document_filename']
                target_filename = None
                is_likely_abs = False

                filing_info = {k: row[k] for k in row.keys()}
                filing_info['date'] = row['filing_date'].strftime('%Y-%m-%d')
                filing_info['form'] = '10-K'
                del filing_info['filing_date']
                found_filings_metadata.append(
                    filing_info)  # Store metadata for all non-ABS/fund results

                if download_docs:
                    # --- Find correct HTM / Check for ABS via index page ---
                    parsed_htm_filename, is_likely_abs = self._find_primary_htm_document(
                        cik, accession_num, '10-K')
                    # ------------------------------------------------------

                    if is_likely_abs:
                        # This secondary check catches ABS filings missed by SIC filter (e.g., if SIC was NULL or wrong)
                        abs_filings_skipped_index += 1
                        logger.debug(
                            f"Skipping download for {accession_num}, flagged as ABS by index page exhibits."
                        )
                        continue  # Skip download for this filing

                    # Determine final filename if not ABS
                    if parsed_htm_filename:
                        target_filename = parsed_htm_filename
                        found_htm += 1
                    else:
                        logger.warning(
                            f"No primary HTM found for {accession_num}. Falling back to DB: {db_filename}"
                        )
                        target_filename = db_filename
                        used_db_filename += 1

                    # Prepare download task if filename found
                    if target_filename:
                        url = self._construct_edgar_document_url(
                            cik, accession_num, target_filename)
                        if url:
                            acc_no_dashes = accession_num.replace('-', '')
                            safe_target_filename = unicodedata.normalize(
                                'NFKD', target_filename).encode(
                                    'ascii', 'ignore').decode('ascii')
                            safe_target_filename = re.sub(
                                r'[^\w\s.-]', '_',
                                safe_target_filename).strip()
                            max_len = 200
                            out_fname = f"{cik}_{acc_no_dashes}_{safe_target_filename}"
                            if len(out_fname) > max_len:
                                base, ext = os.path.splitext(out_fname)
                                out_fname = base[:max_len - len(ext)] + ext
                            out_path = self.tenk_docs_dir / out_fname
                            if not out_path.exists():
                                filings_to_download.append((url, out_path))
                    else:
                        skipped_no_target_filename += 1
                        logger.error(
                            f"No valid target filename for {cik}/{accession_num}. Skipping download."
                        )

            logger.info(
                f"DB results processed: {processed_db}. Found {len(found_filings_metadata)} potential operating company 10-Ks."
            )
            if download_docs:
                logger.info(
                    f"Filename Selection: Found HTM={found_htm}, Used DB Filename={used_db_filename}, Skipped (ABS by Index)={abs_filings_skipped_index}, Skipped (No Filename)={skipped_no_target_filename}, Index Fetch Errors={index_fetch_errors}."
                )
                logger.info(
                    f"Prepared {len(filings_to_download)} docs to download.")

            # APPLY MAX DOWNLOAD LIMIT
            if download_docs and max_downloads is not None and max_downloads > 0:
                if len(filings_to_download) > max_downloads:
                    logger.info(
                        f"Applying limit: Truncating {len(filings_to_download)} to {max_downloads}."
                    )
                    filings_to_download = filings_to_download[:max_downloads]

            # Parallel Downloading
            if download_docs and filings_to_download:
                logger.info(
                    f"Starting download of {len(filings_to_download)} docs using {num_threads} threads..."
                )
                success_count = 0
                failure_count = 0
                with concurrent.futures.ThreadPoolExecutor(
                        max_workers=num_threads) as executor:
                    future_to_task = {
                        executor.submit(self.download_filing_document_worker, task):
                        task
                        for task in filings_to_download
                    }
                    for future in concurrent.futures.as_completed(
                            future_to_task):
                        url, success, error = future.result()
                        if success: success_count += 1
                        else:
                            failure_count += 1
                            logger.warning(
                                f"Download failed: {url}. Error: {error}")
                        processed_count = success_count + failure_count
                        if processed_count % 100 == 0 or processed_count == len(
                                filings_to_download):
                            logger.info(
                                f"Download progress: {processed_count}/{len(filings_to_download)}"
                            )
                logger.info(
                    f"Download finished. Success: {success_count}, Failed: {failure_count}"
                )
            elif download_docs:
                logger.info("No documents needed downloading.")
            else:
                logger.info("Document downloading disabled.")

            # Save Metadata
            meta_path = self.base_dir / f'10k_filings_metadata_{year_start}_{year_end}_filtered.json'  # Indicate filtered
            grouped = collections.defaultdict(lambda: {
                'name': 'N/A',
                'filings': []
            })
            try:
                for f in found_filings_metadata:
                    cik = f['cik']
                if grouped[cik]['name'] == 'N/A':
                    grouped[cik]['name'] = f['name']
                details = {
                    k: v
                    for k, v in f.items() if k not in ['cik', 'name']
                }
                grouped[cik]['filings'].append(details)
                with open(meta_path, 'w', encoding='utf-8') as f:
                    json.dump(dict(grouped), f, indent=2, ensure_ascii=False)
                logger.info(
                    f"Saved metadata ({len(found_filings_metadata)} filtered filings) to {meta_path}"
                )
            except Exception as e:
                logger.error(f"Error saving metadata: {e}", exc_info=True)
            return dict(grouped)
        except Exception as e:
            logger.error(f"Error during 10-K query/download: {e}",
                         exc_info=True)
            return {}
        finally:
            self.session_factory.remove()

    # --- _construct_edgar_document_url ---
    def _construct_edgar_document_url(self,
                                      cik,
                                      accession_number,
                                      filename=None):
        if not cik or not accession_number:
            logger.error(f"URL build fail: CIK/AccNum missing")
            return None
        if not filename:
            logger.error(
                f"URL build fail: Filename missing {cik}/{accession_number}")
            return None
        try:
            cik_no_zeros = str(int(cik))
        except ValueError:
            logger.error(f"URL build fail: Invalid CIK {cik}")
            return None
        acc_no_dashes = accession_number.replace('-', '')
        return f"{self.edgar_archive_base}/{cik_no_zeros}/{acc_no_dashes}/{filename}"

    # --- download_filing_document ---
    def download_filing_document(self, url, output_path):
        if not url:
            logger.warning("Skipping download: URL invalid.")
            return False
        self._respect_rate_limit()
        logger.info(f"Attempting download: {url} -> {output_path}")
        try:
            resp = requests.get(url,
                                headers=self.headers,
                                stream=True,
                                timeout=60)
            if resp.status_code == 404:
                logger.warning(f"Not found (404): {url}")
                return False
            resp.raise_for_status()
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'wb') as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            logger.info(f"Successfully downloaded: {output_path}")
            return True
        except requests.exceptions.Timeout:
            logger.error(f"Timeout download {url}")
            return False
        except requests.exceptions.HTTPError as e:
            logger.warning(
                f"Failed download {url}: HTTP {e.response.status_code}")
            return False
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error {url}: {e}")
            return False
        except Exception as e:
            logger.error(f"Download error {url}: {e}", exc_info=True)
            return False


# --- Main Pipeline Runner ---
def run_pipeline(args):
    """Runs the SEC data pipeline steps based on command-line arguments."""
    logger.info("================ Starting SEC Pipeline Run ================")
    logger.info(f"Run Arguments: {args}")

    engine, session_factory = db.initialize_database()
    if engine is None or session_factory is None:
        logger.critical("DB init failed.")
        return

    pipeline = None
    try:
        pipeline = SECDataPipeline(session_factory,
                                   base_dir_path=Path(args.data_path))
    except ValueError as e:
        logger.critical(f"Pipeline init failed: {e}")
    except Exception as e:
        logger.critical(f"Pipeline init unexpected error: {e}", exc_info=True)
    finally:
        if pipeline is None and engine: engine.dispose()
    if pipeline is None: return

    update_mode = args.mode
    logger.info(f"Pipeline update mode set to: {update_mode}")

    # Data Update Phase
    try:
        is_db_populated = False
        try:
            with session_factory() as session:
                stmt = select(db.Company.cik).limit(1)
            is_db_populated = session.execute(stmt).first() is not None
        except Exception as e:
            logger.error(f"DB check failed: {e}", exc_info=False)

        if not is_db_populated and update_mode not in [
                'bulk', 'bulk_ingest_only'
        ]:
            logger.warning(
                "Database appears empty. Recommend running with --mode bulk or --mode bulk_ingest_only first."
            )

        if update_mode == 'bulk':
            logger.info("Performing FULL bulk process...")
            pipeline.download_and_ingest_bulk(args.bulk_workers)
        elif update_mode == 'bulk_ingest_only':
            logger.info("Performing bulk INGEST ONLY...")
            pipeline.ingest_bulk_submissions_files_to_db(args.bulk_workers)
        elif update_mode == 'incremental':
            logger.info("Performing incremental update via Daily Index...")
            pipeline.incremental_update()

    except Exception as e:
        logger.error(f"Error during data update phase: {e}", exc_info=True)

    # 10-K Query and Download Phase
    logger.info("Proceeding to query/download phase for 10-K filings.")
    try:
        pipeline.query_filter_and_download_10k_filings(
            year_start=args.start_year,
            year_end=args.end_year,
            download_docs=args.download_10k,
            num_threads=args.download_threads,
            max_downloads=args.max_10k_downloads)
    except Exception as e:
        logger.error(f"Error during query/download phase: {e}", exc_info=True)

    if engine:
        logger.info("Disposing database engine.")
        engine.dispose()
    logger.info("================ SEC Pipeline Run Finished ================")


# --- Argument Parsing and Main Execution ---
if __name__ == "__main__":
    # --- START: Path Manipulation for Direct Execution ---
    import sys
    import os
    try:
        script_path = os.path.abspath(__file__)
        project_root = os.path.dirname(
            os.path.dirname(os.path.dirname(script_path)))
        if project_root not in sys.path: sys.path.insert(0, project_root)
    except NameError:
        print("WARNING: __file__ not defined, cannot fix sys.path.",
              file=sys.stderr)
    # --- END: Path Manipulation ---

    import argparse

    default_data_path = os.getenv('DATA_STORAGE_PATH', 'data')
    default_start_year = 2020
    default_end_year = None
    cpu_cores = os.cpu_count() or 1
    default_bulk_workers = max(1, cpu_cores - 1)
    default_download_threads = 10

    parser = argparse.ArgumentParser(
        description="SEC EDGAR Data Pipeline.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--mode",
                        choices=['bulk', 'bulk_ingest_only', 'incremental'],
                        required=True,
                        help="Pipeline execution mode.")
    parser.add_argument("--data-path",
                        default=default_data_path,
                        help="Root directory for storing data.")
    parser.add_argument("--start-year",
                        type=int,
                        default=default_start_year,
                        help="Start year for 10-K query.")
    parser.add_argument(
        "--end-year",
        type=int,
        default=default_end_year,
        help="End year for 10-K query (inclusive). Default: Current year.")
    parser.add_argument("--download-10k",
                        action='store_true',
                        help="Enable downloading of 10-K documents.")
    parser.add_argument("--max-10k-downloads",
                        type=int,
                        default=None,
                        metavar='N',
                        help="Maximum 10-K downloads (most recent first).")
    parser.add_argument("--bulk-workers",
                        type=int,
                        default=default_bulk_workers,
                        help="Processes for parallel bulk parsing.")
    parser.add_argument("--download-threads",
                        type=int,
                        default=default_download_threads,
                        help="Threads for parallel downloading.")
    parser.add_argument("--log-level",
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                        default='INFO',
                        help="Set logging level.")

    args = parser.parse_args()

    log_level = getattr(logging, args.log_level.upper(), logging.INFO)
    logger.setLevel(log_level)
    try:
        logging.getLogger('sec_pipeline.db').setLevel(log_level)
    except:
        pass
    logger.info(f"Log level set to: {args.log_level}")

    try:  # Configure DB logger if available
        db_logger = logging.getLogger('sec_pipeline.db')
        if not db_logger.handlers:
            db_handler = logging.StreamHandler(sys.stdout)
            db_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            db_handler.setFormatter(db_formatter)
            db_logger.addHandler(db_handler)
            db_logger.propagate = False
        db_logger.setLevel(log_level)
    except Exception as log_err:
        logger.warning(f"Could not configure db_logger: {log_err}")

    run_pipeline(args)
    logger.info("Script finished.")
