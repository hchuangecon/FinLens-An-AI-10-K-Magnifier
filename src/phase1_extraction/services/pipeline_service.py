# src/phase1_extraction/services/pipeline.py

import logging
import os
import sys
import time
import zipfile
import collections
import json
from pathlib import Path
from datetime import date, timedelta, datetime
from typing import List, Dict, Optional, Sequence, Tuple, Set

import multiprocessing  # For parallel parsing
import concurrent.futures
import unicodedata  # For filename cleaning
import re  # For filename cleaning

# Core components
from src.config.settings import AppSettings, get_settings
from src.core.rate_limiting import RateLimiter
from src.core.exceptions import *  # Import custom exceptions

# Database components
from src.database import (initialize_database, CompanyRepository,
                          FilingRepository, get_session)
from sqlalchemy.orm import Session, scoped_session

# Extraction components
from src.phase1_extraction.downloaders.base import AbstractDownloader
from src.phase1_extraction.downloaders.bulk import BulkDownloader
from src.phase1_extraction.downloaders.incremental import IncrementalDownloader
from src.phase1_extraction.downloaders.document import DocumentDownloader
from src.phase1_extraction.parsers.base import AbstractParser
from src.phase1_extraction.parsers.json import JSONParser, ParseResult as JSONParseResult
from src.phase1_extraction.parsers.index import IndexParser, ParseResult as IndexParseResult
from src.phase1_extraction.parsers.html import HTMLMetadataParser  # Import the new parser

logger = logging.getLogger(__name__)


# --- Helper function for parallel JSON parsing ---
# This function needs to be defined at the top level for multiprocessing to pickle it.
# It now takes the parser instance as an argument.
def _parse_cik_json_worker(parser: JSONParser,
                           file_path: Path) -> Tuple[Path, JSONParseResult]:
    """Worker function for parsing a single CIK JSON file using the provided parser."""
    try:
        result = parser.parse(file_path)
        return file_path, result
    except Exception as e:
        # Log error here or let the main loop handle it based on return?
        # Returning the exception might be better for central handling.
        logger.error(f"Error parsing {file_path.name} in worker: {e}",
                     exc_info=False)  # Keep log concise
        # Return structure indicating failure for this file
        return file_path, (None, [], 1
                           )  # Company=None, Filings=[], ErrorCount=1


# --- End Helper Function ---


class PipelineService:
    """
    Orchestrates the SEC data extraction, parsing, and storage pipeline.
    """

    def __init__(self):
        """Initializes all components needed for the pipeline."""
        logger.info("Initializing PipelineService...")
        try:
            self.settings: AppSettings = get_settings()
            self.rate_limiter: RateLimiter = RateLimiter(
                self.settings.sec_api.rate_limit)

            # Initialize DB and Repositories
            self.engine, self.session_factory = initialize_database(
                self.settings.database)
            self.company_repo: CompanyRepository = CompanyRepository(
                self.session_factory)
            self.filing_repo: FilingRepository = FilingRepository(
                self.session_factory)
            logger.info("Database and repositories initialized.")

            # Initialize Downloaders
            self.bulk_downloader: BulkDownloader = BulkDownloader(
                self.settings, self.rate_limiter)
            # IncrementalDownloader now handles both daily and quarterly index downloads
            self.index_downloader: IncrementalDownloader = IncrementalDownloader(
                self.settings, self.rate_limiter)
            self.document_downloader: DocumentDownloader = DocumentDownloader(
                self.settings, self.rate_limiter)
            logger.info("Downloaders initialized.")

            # Initialize Parsers
            self.json_parser: JSONParser = JSONParser(self.settings)
            self.index_parser: IndexParser = IndexParser(self.settings)
            self.html_parser: HTMLMetadataParser = HTMLMetadataParser(
                self.settings, self.rate_limiter)
            logger.info("Parsers initialized.")

            # Define data paths from settings
            self.data_path: Path = self.settings.pipeline.data_path
            self.submissions_dir = self.data_path / 'submissions'
            self.tenk_docs_dir = self.data_path / '10k_documents'
            self.document_storage_dir = self.data_path / self.settings.pipeline.document_subdir

            self._ensure_directories_exist()
            logger.info("PipelineService initialized successfully.")

        except FinlensError as fe:  # Catch our custom config/db errors
            logger.critical(
                f"Pipeline initialization failed during setup: {fe}",
                exc_info=True)
            # Re-raise or sys.exit? Re-raise for now.
            raise RuntimeError(f"Pipeline setup failed: {fe}") from fe
        except Exception as e:
            logger.critical(
                f"Unexpected error during PipelineService initialization: {e}",
                exc_info=True)
            raise RuntimeError(
                f"Unexpected error during pipeline setup: {e}") from e

    def _ensure_directories_exist(self):
        """Creates necessary data directories if they don't exist."""
        dirs_to_create = [
            self.data_path, self.submissions_dir, self.tenk_docs_dir
        ]
        logger.debug(f"Ensuring directories exist: {dirs_to_create}")
        try:
            for dir_path in dirs_to_create:
                dir_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(
                f"Failed to create essential directory {dir_path}: {e}",
                exc_info=True)
            raise FileSystemError(
                f"Cannot create required directory: {dir_path}") from e

    # --- Bulk Process ---
    # We might decide those booleans through argument settings (args)
    def run_bulk_process(self,
                         download: bool = True,
                         extract: bool = True,
                         ingest: bool = True):
        """
        Orchestrates the full bulk data acquisition process:
        1. Download submissions.zip.
        2. Extract CIK JSON files.
        3. Parse JSON files and ingest data into the database.
        """
        logger.info("Starting bulk process...")
        zip_path = self.data_path / "submissions.zip"
        extract_target_dir = self.submissions_dir
        overall_success = True

        # 1. Download
        if download:
            logger.info("Step 1/3: Downloading bulk submissions.zip...")
            try:
                success = self.bulk_downloader.download(output_path=zip_path)
                if not success:
                    logger.error(
                        "Bulk download failed. Aborting bulk process.")
                    return False  # Cannot proceed without the zip file
                logger.info("Bulk download successful.")
            except Exception as e:  # Catch exceptions from downloader not returning False
                logger.error(f"Error during bulk download step: {e}",
                             exc_info=True)
                return False

        # 2. Extract
        if extract:
            logger.info(
                f"Step 2/3: Extracting {zip_path} to {extract_target_dir}...")
            try:
                success = self._extract_zip(zip_path, extract_target_dir)
                if not success:
                    logger.error(
                        "Extraction failed. Cannot proceed with ingestion.")
                    return False  # Cannot ingest if extraction fails
                logger.info("Extraction successful.")
            except Exception as e:
                logger.error(f"Error during extraction step: {e}",
                             exc_info=True)
                return False

        # 3. Ingest
        if ingest:
            logger.info(
                "Step 3/3: Ingesting data from extracted JSON files...")
            try:
                success = self._ingest_bulk_json_data(extract_target_dir)
                if not success:
                    logger.error("Ingestion step failed.")
                    overall_success = False  # Mark failure but allow process to finish reporting
                else:
                    logger.info("Ingestion successful.")
            except Exception as e:
                logger.error(f"Error during ingestion step: {e}",
                             exc_info=True)
                overall_success = False

        logger.info(
            f"Bulk process finished. Overall success: {overall_success}")
        return overall_success

    def _extract_zip(self, zip_path: Path, extract_dir: Path) -> bool:
        """Extracts a zip file to a specified directory."""
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

    def _ingest_bulk_json_data(self, source_dir: Path) -> bool:
        """
        Parses CIK JSON files in chunks and ingests data into the database
        to manage memory usage.
        """
        logger.info(
            f"Scanning {source_dir} for CIK*.json files for ingestion...")
        all_cik_files = list(source_dir.rglob("CIK*.json"))
        if not all_cik_files:
            logger.warning(
                f"No CIK JSON files found in {source_dir}. Ingestion skipped.")
            return True  # Not an error if no files found

        total_files_to_process = len(all_cik_files)
        logger.info(
            f"Found {total_files_to_process} CIK JSON files to process.")

        # --- Chunking Configuration ---
        # Adjust this based on available RAM and typical file contents
        # Start with a value like 50,000 or 100,000 and monitor memory.
        file_chunk_size = self.settings.pipeline.bulk_ingest_file_chunk_size
        num_chunks = (total_files_to_process + file_chunk_size -
                      1) // file_chunk_size
        logger.info(
            f"Processing files in {num_chunks} chunks of up to {file_chunk_size} files each."
        )
        # -----------------------------

        total_parse_errors = 0
        total_companies_affected = 0
        total_filings_inserted = 0
        overall_success = True

        num_workers = self.settings.pipeline.bulk_workers

        # --- Process Files in Chunks ---
        for chunk_index in range(num_chunks):
            start_index = chunk_index * file_chunk_size
            end_index = start_index + file_chunk_size
            file_chunk = all_cik_files[start_index:end_index]

            if not file_chunk:
                logger.info(
                    f"Skipping empty chunk {chunk_index + 1}/{num_chunks}.")
                continue

            logger.info(
                f"--- Starting processing for chunk {chunk_index + 1}/{num_chunks} ({len(file_chunk)} files) ---"
            )

            # Reset aggregators for the current chunk
            chunk_company_data: Dict[str, Dict] = {
            }  # Use dict keyed by CIK for latest data
            chunk_filings_data: List[Dict] = []
            chunk_parse_errors: int = 0
            chunk_files_processed: int = 0

            logger.info(
                f"Starting parallel parsing for chunk with {num_workers} workers..."
            )
            tasks = [(self.json_parser, file_path) for file_path in file_chunk]

            try:
                # Process the current chunk in parallel
                with multiprocessing.Pool(processes=num_workers) as pool:
                    # Consider chunksize for starmap if chunks are very large
                    # chunksize_starmap = max(1, len(tasks) // (num_workers * 4))
                    results_iterator = pool.starmap(
                        _parse_cik_json_worker,
                        tasks)  # , chunksize=chunksize_starmap)

                    for file_path, parse_result in results_iterator:
                        chunk_files_processed += 1
                        if chunk_files_processed % (file_chunk_size // 5 if file_chunk_size >= 5 else 1) == 0 or \
                           chunk_files_processed == len(file_chunk):
                            current_total_processed = start_index + chunk_files_processed
                            logger.info(
                                f"Chunk {chunk_index + 1} Parsing: "
                                f"{chunk_files_processed}/{len(file_chunk)} files processed | "
                                f"Overall: {current_total_processed}/{total_files_to_process}..."
                            )

                        company_data, filings_data, parse_errors = parse_result
                        chunk_parse_errors += parse_errors

                        if company_data and company_data.get('cik'):
                            # Store latest parsed data for each CIK within the chunk
                            chunk_company_data[
                                company_data['cik']] = company_data
                        if filings_data:
                            chunk_filings_data.extend(filings_data)

                total_parse_errors += chunk_parse_errors
                logger.info(
                    f"Chunk {chunk_index + 1} parallel parsing complete. "
                    f"Files processed in chunk: {chunk_files_processed}, "
                    f"File/record errors in chunk: {chunk_parse_errors}")

                # --- Database Ingestion for the Current Chunk ---
                logger.info(
                    f"Starting database ingestion phase for chunk {chunk_index + 1}..."
                )

                # Ingest Companies from the chunk using UPSERT
                if chunk_company_data:
                    company_list_chunk = list(chunk_company_data.values())
                    logger.info(
                        f"Upserting {len(company_list_chunk)} companies from chunk {chunk_index + 1}..."
                    )
                    try:
                        # Repository handles internal batching if needed, but main batching is now the file chunk
                        affected_rows = self.company_repo.bulk_upsert(
                            company_list_chunk)
                        total_companies_affected += affected_rows
                        logger.info(
                            f"Company upsert for chunk {chunk_index + 1} complete. MySQL affected rows: {affected_rows}"
                        )
                    except DatabaseError as e:
                        logger.error(
                            f"Company upsert failed during processing of chunk {chunk_index + 1}: {e}. Stopping bulk ingest.",
                            exc_info=True)
                        overall_success = False
                        break  # Stop processing further chunks on DB error
                else:
                    logger.info(
                        f"No valid company data to upsert in chunk {chunk_index + 1}."
                    )

                # Ingest Filings from the chunk using INSERT IGNORE
                if chunk_filings_data:
                    # Deduplication within the chunk - still beneficial
                    unique_filings_dict_chunk = {
                        f['accession_number']: f
                        for f in chunk_filings_data
                        if f.get('accession_number')
                    }
                    final_filings_to_insert_chunk = list(
                        unique_filings_dict_chunk.values())
                    logger.info(
                        f"Inserting {len(final_filings_to_insert_chunk)} unique filings from chunk {chunk_index + 1} (after in-memory deduplication)..."
                    )
                    try:
                        # Repository handles internal batching if needed
                        inserted_count = self.filing_repo.bulk_insert_ignore(
                            final_filings_to_insert_chunk)
                        total_filings_inserted += inserted_count
                        logger.info(
                            f"Filing insert ignore for chunk {chunk_index + 1} complete. Rows actually inserted: {inserted_count}"
                        )
                    except DatabaseError as e:
                        logger.error(
                            f"Filing insert ignore failed during processing of chunk {chunk_index + 1}: {e}. Stopping bulk ingest.",
                            exc_info=True)
                        overall_success = False
                        break  # Stop processing further chunks on DB error
                else:
                    logger.info(
                        f"No valid filing data to insert in chunk {chunk_index + 1}."
                    )

                logger.info(
                    f"--- Finished processing chunk {chunk_index + 1}/{num_chunks} ---"
                )

            except Exception as e:
                logger.error(
                    f"Unexpected error during parallel parsing or ingestion for chunk {chunk_index + 1}: {e}",
                    exc_info=True)
                overall_success = False
                break  # Stop processing further chunks on unexpected error

            # --- Explicitly clear chunk data to potentially help garbage collection ---
            del chunk_company_data
            del chunk_filings_data
            del final_filings_to_insert_chunk  # if created
            del company_list_chunk  # if created
            del tasks
            del results_iterator
            # import gc # Optional: Force garbage collection (usually not needed)
            # gc.collect()
            # -------------------------------------------------------------------------

            if not overall_success:  # Check if loop broke due to error
                break

        # --- End of Chunk Processing Loop ---

        logger.info("=" * 50)
        if overall_success:
            logger.info(
                f"Bulk ingestion process completed successfully across all chunks."
            )
        else:
            logger.error(
                f"Bulk ingestion process failed or stopped prematurely.")

        logger.info(f"Final Summary:")
        logger.info(
            f"  Total Files Processed: Approximately {start_index + chunk_files_processed if 'start_index' in locals() else 0}/{total_files_to_process}"
        )
        logger.info(f"  Total Parse Errors Encountered: {total_parse_errors}")
        logger.info(
            f"  Total Company Rows Affected (MySQL Count): {total_companies_affected}"
        )
        logger.info(f"  Total Filing Rows Inserted: {total_filings_inserted}")
        logger.info("=" * 50)

        return overall_success

    # --- End of Bulk Process ---

    # --- Incremental Update ---
    def run_incremental_update(self, days_to_check: Optional[int] = None):
        """
        Orchestrates downloading and processing daily index files for recent filings.
        Adds new filings and associated company data to the database.

        Args:
            days_to_check: How many past days (including today) to check indices for.
        """
        # Use setting as default if argument is None
        _days_to_check = days_to_check if days_to_check is not None else self.settings.pipeline.incremental_days_to_check
        logger.info(
            f"Starting incremental update, checking indices for the last {_days_to_check} days..."
        )
        overall_success = True  # Tracks if recoverable errors occurred

        # 1. Determine dates to check
        dates_to_process: List[date] = []
        today = date.today()
        for i in range(_days_to_check):
            check_date = today - timedelta(days=i)
            dates_to_process.append(check_date)
        logger.info(
            f"Checking dates from {min(dates_to_process)} to {max(dates_to_process)}."
        )

        # 2. Download and Parse Indices
        all_filings_from_indices: Dict[str, Dict] = {
        }  # Key: acc_no, Value: filing dict
        for target_date in sorted(dates_to_process):  # Process chronologically
            logger.debug(f"Processing index for date: {target_date}")
            index_content: Optional[str] = None
            try:
                # Call the downloader for the specific date
                index_content = self.index_downloader.download(target_date)

                if index_content:
                    # If download successful, parse the content
                    # No target_forms filter needed here - get all filings first
                    parsed_filings = self.index_parser.parse(
                        index_content,
                        source_description=f"Daily-{target_date.isoformat()}")
                    if parsed_filings:
                        # Add/overwrite results using accession number as key to auto-deduplicate across days
                        for filing_dict in parsed_filings:
                            if filing_dict.get("accession_number"):
                                all_filings_from_indices[filing_dict[
                                    "accession_number"]] = filing_dict
                        logger.debug(
                            f"Parsed {len(parsed_filings)} filings from index {target_date}."
                        )
                # else: index file not found (404), normal, logged by downloader

            except (RequestTimeoutError, DownloadError) as e:
                logger.error(
                    f"Failed to download index for {target_date}: {e}. Skipping date."
                )
                overall_success = False  # Mark potential issue
            except (ParsingError,
                    IndexParsingError) as e:  # Catch specific parsing errors
                logger.error(
                    f"Failed to parse index content for {target_date}: {e}. Skipping date."
                )
                overall_success = False
            except Exception as e:
                logger.error(
                    f"Unexpected error processing date {target_date}: {e}",
                    exc_info=True)
                overall_success = False

        if not all_filings_from_indices:
            logger.info("No filings found in recent indices to process.")
            # Return True because no fundamental error occurred, just no new data
            return True

        logger.info(
            f"Collected {len(all_filings_from_indices)} unique filing records from recent indices."
        )
        filings_list_to_check = list(all_filings_from_indices.values())

        # 3. Add New Filings to Database
        logger.info("Inserting/Ignoring filings into database...")
        try:
            # Use bulk_insert_ignore - repository handles batching
            inserted_count = self.filing_repo.bulk_insert_ignore(
                filings_list_to_check)
            logger.info(
                f"Database filing update complete. Actually inserted: {inserted_count} new filings."
            )
            # Even if inserted_count is 0, proceed to check companies, as company data might need update
        except DatabaseError as e:
            logger.error(
                f"Database error during filing insertion: {e}. Aborting incremental update.",
                exc_info=True)
            return False  # Cannot reliably continue without filings table updated

        # 4. Identify Potentially New or Changed CIKs
        # Get all unique CIKs involved in the recently downloaded filings
        ciks_in_recent_filings = {
            f['cik']
            for f in filings_list_to_check if f.get('cik')
        }
        if not ciks_in_recent_filings:
            logger.info(
                "No valid CIKs found in recent filings. Skipping company update."
            )
            return overall_success  # Return status from download/parse/filing phase

        logger.info(
            f"Checking/updating company data for {len(ciks_in_recent_filings)} involved CIKs."
        )

        # 5. Fetch Company Data via API (for all involved CIKs - simpler than finding just new ones)
        # We fetch for all involved CIKs because even existing companies might have updated metadata (e.g., name change, SIC)
        # The bulk_upsert in the repository handles inserting new ones and updating existing ones.
        companies_to_upsert: List[Dict] = []
        api_fetch_errors = 0
        ciks_processed_count = 0
        total_ciks_to_process = len(ciks_in_recent_filings)

        for cik in ciks_in_recent_filings:
            ciks_processed_count += 1
            if ciks_processed_count % 50 == 0:  # Log progress periodically
                logger.info(
                    f"Company API fetch progress: {ciks_processed_count}/{total_ciks_to_process}"
                )

            # Use a helper method to fetch and parse company details
            company_details = self._fetch_company_data_from_api(cik)
            if company_details:
                companies_to_upsert.append(company_details)
            else:
                api_fetch_errors += 1
                # Decide if API errors should halt the process or just be logged
                # For now, log and continue.
                overall_success = False  # Mark potential issue if API fails

        logger.info(
            f"Finished fetching company data via API. Success: {len(companies_to_upsert)}, Failed/Skipped: {api_fetch_errors}"
        )

        # 6. Upsert Company Data
        if companies_to_upsert:
            logger.info(
                f"Upserting {len(companies_to_upsert)} company records...")
            try:
                # Use bulk_upsert - repository handles batching and insert/update logic
                affected_rows = self.company_repo.bulk_upsert(
                    companies_to_upsert)
                logger.info(
                    f"Company upsert complete. MySQL affected rows: {affected_rows}"
                )
            except DatabaseError as e:
                logger.error(
                    f"Database error during company upsert: {e}. Incremental update finished with errors.",
                    exc_info=True)
                return False  # Treat DB error as fatal
        else:
            logger.info(
                "No company data successfully fetched from API to upsert.")

        logger.info(
            f"Incremental update finished. Overall success: {overall_success}")
        return overall_success

    def _fetch_company_data_from_api(self, cik: str) -> Optional[Dict]:
        """
        Fetches company submission metadata from the data.sec.gov API for a given CIK.
        Parses the JSON and returns a dictionary suitable for the Company model.
        Handles rate limiting and basic API errors.
        """
        # Ensure CIK is zero-padded to 10 digits for the API URL
        if not cik.isdigit() or len(cik) > 10:
            logger.warning(f"Invalid CIK format for API lookup: {cik}")
            return None
        cik_padded = cik.zfill(10)
        api_url = f"{self.submissions_api_base}{cik_padded}.json"

        retries = 2  # Number of retries on transient errors
        for attempt in range(retries + 1):
            try:
                # Use the base downloader's _make_request helper with API headers
                logger.debug(
                    f"Fetching company data for CIK {cik} from {api_url}")
                response = self.bulk_downloader._make_request(  # Use any downloader instance for the helper
                    url=api_url,
                    headers=self.bulk_downloader.
                    api_headers,  # Use API headers
                    stream=False,
                    timeout=30  # Shorter timeout for API calls
                )

                # Parse JSON response
                api_data = response.json()

                # Extract data similarly to JSONParser (keep logic consistent)
                company_name = api_data.get('entityName', '') or api_data.get(
                    'name', '') or f"Company CIK {cik}"
                addresses = api_data.get('addresses', {})
                business_address = addresses.get('business', {}) if isinstance(
                    addresses, dict) else {}

                owner_exists_raw = api_data.get(
                    'insiderTransactionForOwnerExists')
                issuer_exists_raw = api_data.get(
                    'insiderTransactionForIssuerExists')
                owner_exists = bool(owner_exists_raw) if owner_exists_raw in [
                    0, 1
                ] else None
                issuer_exists = bool(
                    issuer_exists_raw) if issuer_exists_raw in [0, 1] else None

                company_details = {
                    'cik':
                    cik,  # Use original CIK, not padded
                    'name':
                    company_name.strip(),
                    'sic':
                    api_data.get('sic'),
                    'entity_type':
                    api_data.get('entityType'),
                    'sic_description':
                    api_data.get('sicDescription'),
                    'insider_trade_owner':
                    owner_exists,  # Use renamed fields
                    'insider_trade_issuer':
                    issuer_exists,  # Use renamed fields
                    'phone':
                    api_data.get('phone'),
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
                return company_details  # Success

            except NotFoundError:
                logger.warning(
                    f"Company data not found via API for CIK {cik} (404)")
                return None  # CIK might be invalid or have no submissions data
            except (RequestTimeoutError, DownloadError) as e:
                logger.warning(
                    f"API request failed for CIK {cik}: {e}. Attempt {attempt + 1}/{retries + 1}"
                )
                if attempt < retries:
                    time.sleep(1.5**attempt)  # Exponential backoff
                    continue  # Retry
                else:
                    logger.error(
                        f"API request failed for CIK {cik} after multiple retries."
                    )
                    return None  # Failed after retries
            except json.JSONDecodeError as e:
                logger.error(
                    f"Failed to decode JSON response from API for CIK {cik}: {e}"
                )
                return None  # Bad response format
            except Exception as e:
                logger.error(
                    f"Unexpected error fetching company data for CIK {cik}: {e}",
                    exc_info=True)
                return None  # Unknown error

        return None  # Should not be reached if retries implemented correctly, but as fallbac

    # --- End of Incremental Update ---

    # --- Historical Backfill ---
    def run_historical_backfill(self,
                                start_year: int,
                                end_year: int,
                                forms_to_include: Optional[Set[str]] = None):
        """
        Orchestrates downloading and processing quarterly index files for historical data,
        filtering for specific forms and adding them to the filings database.

        Args:
            start_year: The first year to process (inclusive).
            end_year: The last year to process (inclusive).
            forms_to_include: A set of upper-case form types to keep (e.g., {'10-K', '10-K/A'}).
                              If None, all filings found will be processed (use with caution).
        """

        # Use setting as default if argument is None
        _forms_to_include = forms_to_include if forms_to_include is not None else self.settings.pipeline.backfill_target_forms
        # Note: _forms_to_include can still be None if setting isn't set and no arg given
        logger.info(
            f"Starting historical backfill from {start_year} to {end_year}.")
        if _forms_to_include:
            logger.info(f"Filtering for forms: {_forms_to_include}")
        else:
            logger.warning(
                "No form filter specified for backfill, processing all forms found in indices."
            )

        overall_success = True
        total_filings_added = 0

        # Iterate through years and quarters
        for year in range(start_year, end_year + 1):
            for quarter in range(1, 5):  # Q1, Q2, Q3, Q4
                source_desc = f"{year}-Q{quarter}"
                logger.info(f"Processing {source_desc}...")
                qtr_filings_list: List[Dict] = []
                qtr_content: Optional[str] = None

                try:
                    # 1. Download and decompress quarterly index content
                    qtr_content = self.index_downloader.download_quarterly_index_content(
                        year, quarter)

                    if qtr_content:
                        # 2. Parse the content, applying form filter
                        parsed_filings = self.index_parser.parse(
                            qtr_content,
                            source_description=source_desc,
                            target_forms=forms_to_include  # Pass the filter here
                        )
                        qtr_filings_list.extend(parsed_filings)
                        logger.info(
                            f"Parsed {len(qtr_filings_list)} relevant filings from {source_desc}."
                        )
                    # else: File not found (404), logged by downloader, continue to next quarter

                    # 3. Insert filings for this quarter
                    if qtr_filings_list:
                        logger.info(
                            f"Inserting {len(qtr_filings_list)} filings for {source_desc}..."
                        )
                        try:
                            inserted_count = self.filing_repo.bulk_insert_ignore(
                                qtr_filings_list)
                            logger.info(
                                f"Database insert for {source_desc} complete. Actually inserted: {inserted_count}"
                            )
                            total_filings_added += inserted_count
                        except DatabaseError as db_err:
                            logger.error(
                                f"Database error inserting filings for {source_desc}: {db_err}. Skipping quarter.",
                                exc_info=True)
                            overall_success = False
                            continue  # Skip to next quarter on DB error for this batch
                    else:
                        logger.info(
                            f"No relevant filings to insert for {source_desc}."
                        )

                except (RequestTimeoutError, DownloadError) as dl_err:
                    logger.error(
                        f"Failed to download/process index for {source_desc}: {dl_err}. Skipping quarter."
                    )
                    overall_success = False
                except (ParsingError, IndexParsingError) as parse_err:
                    logger.error(
                        f"Failed to parse index content for {source_desc}: {parse_err}. Skipping quarter."
                    )
                    overall_success = False
                except Exception as e:
                    logger.error(
                        f"Unexpected error processing {source_desc}: {e}",
                        exc_info=True)
                    overall_success = False

        logger.info(
            f"Historical backfill finished for {start_year}-{end_year}. "
            f"Total new filings inserted: {total_filings_added}. Overall success: {overall_success}"
        )
        return overall_success

    # --- End of Historical Backfill ---

    # --- Document Download ---
    def _sanitize_filename(self, filename: str, max_len: int = 200) -> str:
        """Cleans and sanitizes a filename for safe local storage."""
        # Normalize unicode characters
        safe_name = unicodedata.normalize('NFKD', filename).encode(
            'ascii', 'ignore').decode('ascii')
        # Replace invalid chars with underscore
        safe_name = re.sub(r'[^\w\s.-]', '_', safe_name).strip()
        # Reduce multiple spaces/underscores
        safe_name = re.sub(r'[_ ]+', '_', safe_name)
        # Truncate if too long (preserving extension)
        if len(safe_name) > max_len:
            base, ext = os.path.splitext(safe_name)
            ext_len = len(ext)
            base_len_allowed = max_len - ext_len
            if base_len_allowed < 1:
                safe_name = safe_name[:max_len]
            else:
                safe_name = base[:base_len_allowed] + ext
        return safe_name

    def _prepare_download_task(
            self, filing_info: Dict, target_forms: Set[str]
    ) -> Optional[Tuple[str, str, str, str, Path]]:
        """
        Prepares details needed for downloading a single filing's primary document.
        Finds filename via HTML parser, builds URL and output path.

        Returns:
            Tuple (cik, accession_number, primary_filename, download_url, output_path)
            if document found and ready, else None.
        """
        cik = filing_info.get('cik')
        accession_number = filing_info.get('accession_number')

        if not cik or not accession_number:
            logger.warning(
                f"Skipping download prep due to missing CIK or Accession Number: {filing_info}"
            )
            return None

        try:
            # Find the primary HTM filename using the HTML parser
            primary_filename, is_likely_abs = self.html_parser.find_primary_document(
                cik=cik,
                accession_number=accession_number,
                target_form_types=target_forms)

            if is_likely_abs:
                logger.info(
                    f"Skipping download prep for {cik}/{accession_number}: Flagged as likely ABS by HTML parser."
                )
                return None  # Don't prepare download for likely ABS filings (No real business operations)

            if primary_filename:
                # Construct output path
                safe_filename = self._sanitize_filename(primary_filename)
                acc_no_dashes = accession_number.replace('-', '')
                # Define output filename structure (e.g., CIK_ACCNO_FILENAME.htm)
                output_filename = f"{cik}_{acc_no_dashes}_{safe_filename}"
                # Use the configured document storage directory
                output_path = self.document_storage_dir / output_filename

                # Construct download URL
                download_url = self.document_downloader._build_document_url(
                    cik, accession_number, primary_filename)

                if download_url:
                    # Return all necessary details for the download job
                    return cik, accession_number, primary_filename, download_url, output_path
                else:
                    logger.error(
                        f"Could not construct download URL for {cik}/{accession_number}/{primary_filename}"
                    )
                    return None
            else:
                # Filename not found (already logged by html_parser)
                return None

        except (ValueError, NotFoundError, NetworkError, ParsingError,
                RequestTimeoutError) as e:
            # Log specific errors encountered during filename finding/prep
            logger.error(
                f"Failed to find/prepare document for {cik}/{accession_number}: {e}",
                exc_info=False)
            return None
        except Exception as e:
            # Log unexpected errors during preparation
            logger.error(
                f"Unexpected error preparing download for {cik}/{accession_number}: {e}",
                exc_info=True)
            return None

    def download_filing_documents(
            self,
            filings_to_process: Sequence[Dict],
            target_forms: Set[str] = {'10-K', '10-K/A'},
            num_threads: Optional[int] = None,
            max_downloads: Optional[int] = None,
            skip_existing: bool = True):  # Added flag to control skipping
        """
        Downloads specific document files (e.g., 10-Ks, 10-K/As) for a given list of filings.
        Finds primary filename via HTMLMetadataParser, downloads via DocumentDownloader in parallel.

        Args:
            filings_to_process: A sequence of dictionaries, each containing at least
                                'cik' and 'accession_number'. Assumes this list
                                is already filtered for relevant companies (e.g., non-ABS).
            target_forms: The set of form types to find the primary document for.
            num_threads: Number of parallel download threads. Defaults to pipeline setting.
            max_downloads: Optional limit on the number of documents to download.
            skip_existing: If True, checks if the output file exists and skips download if it does.
        """
        # Use setting as default if argument is None
        _target_forms = target_forms if target_forms is not None else self.settings.pipeline.target_primary_doc_forms
        _num_threads = num_threads if num_threads is not None else self.settings.pipeline.download_threads

        logger.info(
            f"Preparing to download documents for up to {len(filings_to_process)} filings "
            f"(Target forms: {_target_forms}) using {_num_threads} threads.")

        # --- Stage 1: Prepare Download Tasks ---
        download_tasks: List[Tuple[str, str, str, str, Path]] = [
        ]  # (cik, acc_no, filename, url, output_path)
        prep_errors = 0
        skipped_existing = 0

        for filing_info in filings_to_process:
            task_details = self._prepare_download_task(filing_info,
                                                       target_forms)
            if task_details:
                output_path_to_check = task_details[
                    4]  # Path is the 5th element (index 4)
                if skip_existing and output_path_to_check.exists():
                    logger.debug(
                        f"Skipping download, file exists: {output_path_to_check}"
                    )
                    skipped_existing += 1
                else:
                    download_tasks.append(task_details)
            else:
                prep_errors += 1

            # Apply max_downloads limit *after* potential skipping
            if max_downloads is not None and len(
                    download_tasks) >= max_downloads:
                logger.info(
                    f"Reached max_downloads limit ({max_downloads}) for actual downloads. Stopping task preparation."
                )
                break  # Stop adding new tasks

        logger.info(f"Prepared {len(download_tasks)} download tasks. "
                    f"Skipped {skipped_existing} existing files. "
                    f"Encountered {prep_errors} errors during preparation.")

        if not download_tasks:
            logger.info("No documents need downloading.")
            return 0, prep_errors  # Return success=0, fail=prep_errors? Or just 0,0? Let's do 0,0

        # --- Stage 2: Execute Downloads in Parallel ---
        logger.info(
            f"Starting parallel download of {len(download_tasks)} documents..."
        )
        success_count = 0
        failure_count = prep_errors  # Start failure count with prep errors

        with concurrent.futures.ThreadPoolExecutor(
                max_workers=num_threads) as executor:
            # Map futures back to output_path for logging
            future_to_path: Dict[concurrent.futures.Future, Path] = {}

            for task_tuple in download_tasks:
                # Unpack arguments needed by DocumentDownloader.download
                submit_cik, submit_acc_no, submit_filename, submit_url, submit_output_path = task_tuple

                future = executor.submit(
                    self.document_downloader.download,  # The method to call
                    cik=submit_cik,
                    accession_number=submit_acc_no,
                    filename=submit_filename,
                    output_path=submit_output_path)
                future_to_path[
                    future] = submit_output_path  # Map future to output path

            processed_count = 0
            total_tasks = len(download_tasks)
            for future in concurrent.futures.as_completed(future_to_path):
                output_path = future_to_path[future]
                processed_count += 1
                try:
                    success = future.result(
                    )  # Get result (True/False) from downloader
                    if success:
                        success_count += 1
                        logger.debug(
                            f"Download successful: {output_path.name}")
                    else:
                        failure_count += 1
                        logger.warning(
                            f"Download reported as failed for: {output_path.name}"
                        )

                    # Log progress periodically
                    if processed_count % 100 == 0 or processed_count == total_tasks:
                        logger.info(
                            f"Download progress: {processed_count}/{total_tasks} "
                            f"(Success: {success_count}, Failed: {failure_count})"
                        )

                except Exception as exc:
                    logger.error(
                        f'Task for {output_path.name} generated an exception during future.result(): {exc}',
                        exc_info=True)
                    failure_count += 1

        logger.info(
            f"Document download finished. Success: {success_count}, Failed: {failure_count} (incl. prep errors)."
        )
        # Return counts for potential reporting
        return success_count, failure_count

    # --- End of Document Download ---

    def close(self):
        """Clean up resources, like the database engine."""
        if hasattr(self, 'engine') and self.engine:
            logger.info("Disposing database engine.")
            self.engine.dispose()
