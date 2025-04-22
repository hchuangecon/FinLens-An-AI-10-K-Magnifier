# test_find_htm.py
import requests
import logging
import time
import re
import os
from threading import Lock
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# --- Basic Logging Setup ---
logging.basicConfig(
    level=logging.DEBUG,  # Set to DEBUG to see detailed logs from the function
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]  # Log to console
)
logger = logging.getLogger('test_find_htm')

# --- Load Environment Variables for User-Agent ---
load_dotenv()


# --- Minimal SECDataPipeline for Testing ---
class MinimalPipeline:

    def __init__(self):
        user_agent = os.getenv('SEC_USER_AGENT', None)
        if not user_agent:
            logger.error("FATAL: SEC_USER_AGENT environment variable not set.")
            raise ValueError("SEC_USER_AGENT required.")
        self.headers = {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"
        }
        self.last_request_time = 0
        self._rate_limit_lock = Lock()
        logger.info(f"Using User-Agent: {user_agent}")

    def _respect_rate_limit(self, delay=0.11):
        with self._rate_limit_lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            target_delay = max(delay, 0.11)
            if time_since_last_request < target_delay:
                time.sleep(target_delay - time_since_last_request)
            self.last_request_time = time.time()

    def _find_primary_htm_document(
            self,
            cik: str,
            accession_number: str,
            target_form_type: str = '10-K') -> str | None:
        """
        Fetches the filing index page and attempts to find the filename
        of the primary HTML document matching the target_form_type.
        Handles cases where iXBRL text might be concatenated.
        """
        logger.debug(
            f"Attempting to find primary HTM for {cik}/{accession_number} (target: {target_form_type})"
        )
        acc_no_dashes = accession_number.replace('-', '')
        try:
            cik_int_str = str(int(cik))
        except ValueError:
            logger.error(f"Invalid CIK: {cik}")
            return None

        index_page_url = f"https://www.sec.gov/Archives/edgar/data/{cik_int_str}/{acc_no_dashes}/{accession_number}-index.html"
        logger.info(f"Fetching URL: {index_page_url}")

        try:
            self._respect_rate_limit()
            response = requests.get(index_page_url,
                                    headers=self.headers,
                                    timeout=30)
            logger.info(f"HTTP Status Code: {response.status_code}")
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'lxml')

            logger.debug("Searching for 'Document Format Files' table...")
            doc_table = soup.find('table', class_='tableFile')
            if not doc_table:
                logger.debug(
                    "Table class 'tableFile' not found. Trying header search..."
                )
                header = soup.find(['h2', 'div'],
                                   string=re.compile(r'Document Format Files',
                                                     re.IGNORECASE))
                if not header:
                    logger.debug(
                        "Header not found. Trying summary attribute...")
                    doc_table = soup.find('table',
                                          summary=re.compile(
                                              "Document Format Files",
                                              re.IGNORECASE))
                else:
                    logger.debug(
                        "Found header, looking for next table sibling...")
                    doc_table = header.find_next_sibling('table')

            if not doc_table:
                logger.warning(
                    f"Could not find document table: {index_page_url}")
                return None
            else:
                logger.debug("Found document table.")

            rows = doc_table.find_all('tr')
            logger.debug(f"Found {len(rows)} rows in table.")
            if len(rows) < 2:
                logger.warning(f"No data rows in table: {index_page_url}")
                return None

            target_form_variations = {
                target_form_type, target_form_type + '/A'
            }
            logger.debug(f"Targets: {target_form_variations}")

            for i, row in enumerate(rows[1:]):
                cells = row.find_all('td')
                logger.debug(f"Row {i+1}, {len(cells)} cells.")
                if len(cells) >= 4:
                    try:
                        seq = cells[0].get_text(strip=True)
                        description = cells[1].get_text(strip=True)
                        doc_type = cells[3].get_text(strip=True)

                        # --- FIXED FILENAME EXTRACTION ---
                        link_tag = cells[2].find(
                            'a', href=True)  # Find link within 3rd cell
                        if link_tag:
                            doc_filename = link_tag.get_text(strip=True)
                        else:
                            # Fallback if no <a> tag is present in the cell
                            doc_filename = cells[2].get_text(strip=True)
                        # --- END FIX ---

                        logger.debug(
                            f"  Row Data: Seq={seq}, Desc='{description}', File='{doc_filename}', Type='{doc_type}'"
                        )

                        # Check type and extension
                        if doc_type in target_form_variations and doc_filename.lower(
                        ).endswith(('.htm', '.html')):
                            logger.info(
                                f"  MATCH FOUND: Returning filename '{doc_filename}'"
                            )
                            return doc_filename
                        else:
                            logger.debug(
                                f"  Row did not match criteria (Type in {target_form_variations}? {'Yes' if doc_type in target_form_variations else 'No'}. Ends with .htm/.html? {'Yes' if doc_filename.lower().endswith(('.htm', '.html')) else 'No'})."
                            )

                    except IndexError:
                        logger.warning(
                            f"  Skipping row {i+1} index error: {row}")
                    except Exception as cell_err:
                        logger.warning(
                            f"  Error processing cells in row {i+1}: {cell_err}",
                            exc_info=False)
                else:
                    logger.warning(f"  Skipping row {i+1} with < 4 cells.")

            logger.warning(
                f"Could not find matching document on index page: {index_page_url}"
            )
            return None

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed fetch index page {index_page_url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing index page {index_page_url}: {e}",
                         exc_info=True)
            return None


# --- Main Test Execution ---
if __name__ == "__main__":
    # --- Target Filing ---
    test_cik = '0002042453'
    test_accession_number = '0002042453-25-000012'  # The one with lake_10k.htm
    test_form = '10-K'

    logger.info(f"--- Testing _find_primary_htm_document ---")
    logger.info(f"Target CIK: {test_cik}")
    logger.info(f"Target Accession: {test_accession_number}")
    logger.info(f"Target Form: {test_form}")
    logger.info("-" * 20)

    try:
        pipeline_tester = MinimalPipeline()
        result_filename = pipeline_tester._find_primary_htm_document(
            test_cik, test_accession_number, test_form)

        logger.info("-" * 20)
        if result_filename:
            logger.info(
                f"SUCCESS: Found primary HTM filename: '{result_filename}'")
        else:
            logger.error(
                f"FAILURE: Did not find primary HTM filename for {test_form}.")
    except ValueError as e:
        logger.error(f"Test script failed: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during test: {e}",
                     exc_info=True)
