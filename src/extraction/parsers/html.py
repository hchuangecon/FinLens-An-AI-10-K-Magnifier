# src/extraction/parsers/html.py

import logging
import re
from typing import Optional, Tuple, Set
import requests  # For direct request and exceptions
from bs4 import BeautifulSoup

# Core components needed for making requests
from src.config.settings import AppSettings
from src.core.rate_limiting import RateLimiter
from src.core.exceptions import NetworkError, ParsingError, NotFoundError, RequestTimeoutError

logger = logging.getLogger(__name__)


class HTMLMetadataParser:
    """
    Parses specific SEC HTML pages, like the filing index page,
    to extract metadata (e.g., primary document filename).
    """

    # Doesn't inherit from AbstractParser as its input isn't just source data,
    # it needs identifiers to fetch the source first.

    def __init__(self, settings: AppSettings, rate_limiter: RateLimiter):
        """
        Initializes the parser with settings and rate limiter for requests.
        """
        self.settings = settings
        self.rate_limiter = rate_limiter
        self.sec_api_settings = settings.sec_api  # Convenience alias
        self.headers = {  # Standard headers for SEC web requests
            "User-Agent": self.sec_api_settings.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"
        }
        logger.info(f"{self.__class__.__name__} initialized.")

    def _make_request_internal(self, url: str, timeout: int = 30):
        """Internal helper for making rate-limited requests specifically for this parser."""
        self.rate_limiter.wait()
        logger.debug(f"Fetching HTML metadata from: {url}")
        try:
            response = requests.get(url, headers=self.headers, timeout=timeout)
            # Check for 4xx/5xx specifically here
            if response.status_code == 404:
                raise NotFoundError(f"HTML page not found at {url}", url=url)
            elif response.status_code >= 400:
                # Raise a generic NetworkError for other client/server errors
                raise NetworkError(f"HTTP error {response.status_code}",
                                   url=url,
                                   status_code=response.status_code)
            return response
        except requests.exceptions.Timeout:
            logger.error(f"Timeout requesting {url}")
            raise RequestTimeoutError(
                f"Request timed out after {timeout} seconds", url=url)
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception for {url}: {e}")
            raise NetworkError(f"Network request failed: {e}", url=url)
        except Exception as e:
            logger.error(f"Unexpected error during request to {url}: {e}",
                         exc_info=True)
            raise NetworkError(f"Unexpected request error: {e}", url=url)

    def find_primary_document(
        self,
        cik: str,
        accession_number: str,
        target_form_types: Set[str] = {'10-K', '10-K/A'}
    ) -> Tuple[Optional[str], bool]:
        """
        Finds the primary document filename (e.g., *.htm) for a target form type
        by parsing the filing's index page (ACCESSION-NUMBER-index.html).
        Also checks for common ABS exhibit types.

        Args:
            cik: The CIK of the company.
            accession_number: The accession number of the filing (with dashes).
            target_form_types: A set of upper-case form types to search for (e.g., {'10-K', '10-K/A'}).

        Returns:
            A tuple: (primary_filename_or_None, is_likely_abs_flag)
            - primary_filename_or_None: The string filename if found and not ABS, else None.
            - is_likely_abs_flag: True if an ABS indicator exhibit was found, False otherwise.

        Raises:
            ValueError: If CIK/Accession number is invalid.
            NotFoundError: If the index HTML page itself returns 404.
            NetworkError: For other HTTP/network issues fetching the index page.
            ParsingError: If the HTML structure is unexpected (e.g., table not found).
        """
        logger.debug(
            f"Finding primary doc for CIK {cik}, Acc# {accession_number}, TargetForms: {target_form_types}"
        )

        # --- 1. Construct Index Page URL ---
        if not cik or not accession_number:
            raise ValueError("CIK and Accession Number are required.")
        try:
            cik_int_str = str(int(cik))  # Remove leading zeros
        except ValueError:
            raise ValueError(f"Invalid CIK format: {cik}")
        acc_no_dashes = accession_number.replace('-', '')
        index_page_url = f"{self.sec_api_settings.base_url}/Archives/edgar/data/{cik_int_str}/{acc_no_dashes}/{accession_number}-index.html"
        logger.debug(f"Constructed index page URL: {index_page_url}")

        # --- 2. Fetch Index Page HTML ---
        try:
            response = self._make_request_internal(index_page_url)
            html_content = response.content  # Get raw bytes
        except NotFoundError:
            logger.warning(
                f"Filing index page not found (404): {index_page_url}")
            return None, False  # Treat as non-ABS if index not found
        except (NetworkError, RequestTimeoutError) as e:
            logger.error(
                f"Failed to download index page {index_page_url}: {e}")
            # Re-raise network errors as they prevent parsing
            raise

        # --- 3. Parse HTML with BeautifulSoup ---
        try:
            # Use lxml for speed and robustness
            soup = BeautifulSoup(html_content, 'lxml')

            # Find the 'Document Format Files' table (copied selectors from original code)
            doc_table = soup.find('table', class_='tableFile')
            if not doc_table:
                # Try alternative methods used in original code if needed
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
                logger.warning(
                    f"Cannot find 'Document Format Files' table on index page: {index_page_url}"
                )
                # Raise specific parsing error
                raise ParsingError("Could not find document table in HTML",
                                   source=index_page_url)

            # --- 4. Iterate through Table Rows ---
            rows = doc_table.find_all('tr')
            if len(rows) < 2:  # Need header + at least one data row
                logger.warning(
                    f"No data rows found in document table: {index_page_url}")
                return None, False  # No document found

            # Define ABS exhibit prefixes (consider making this configurable via settings?)
            abs_exhibit_prefixes = ('EX-33', 'EX-34', 'EX-35', 'EX-1122',
                                    'EX-1123')
            parsed_htm_filename: Optional[str] = None
            is_likely_abs: bool = False

            for i, row in enumerate(rows[1:]):  # Skip header row
                cells = row.find_all('td')
                if len(
                        cells
                ) >= 4:  # Need at least Seq, Description, Document, Type columns
                    try:
                        doc_type_cell_text = cells[3].get_text(
                            strip=True).upper()

                        # Check for ABS exhibits first
                        if any(
                                doc_type_cell_text.startswith(prefix)
                                for prefix in abs_exhibit_prefixes):
                            is_likely_abs = True
                            logger.debug(
                                f"Detected likely ABS filing {accession_number} due to Exhibit: {doc_type_cell_text}"
                            )
                            # If we find ABS, we know the outcome for the flag, but keep searching for primary doc just in case?
                            # Original code seemed to break early. Let's break too for efficiency.
                            break  # Found ABS indicator, no need to check further rows for ABS flag

                        # If not ABS yet, check if this row is our target primary document
                        if not parsed_htm_filename:
                            # Ensure target_form_types contains upper-case strings
                            normalized_target_forms = {
                                f.upper()
                                for f in target_form_types
                            }
                            if doc_type_cell_text in normalized_target_forms:
                                link_tag = cells[2].find('a', href=True)
                                doc_filename = link_tag.get_text(
                                    strip=True
                                ) if link_tag else cells[2].get_text(
                                    strip=True)

                                # Check if it looks like an HTML file
                                if doc_filename.lower().endswith(
                                    ('.htm', '.html')):
                                    parsed_htm_filename = doc_filename
                                    logger.debug(
                                        f"Found candidate primary document '{parsed_htm_filename}' of type '{doc_type_cell_text}'"
                                    )
                                    # Don't break yet, let loop continue in case an ABS exhibit appears later in the table

                    except Exception as cell_err:
                        # Log error parsing a specific row but continue trying other rows
                        logger.warning(
                            f"Error parsing cell data in row {i+1} on {index_page_url}: {cell_err}",
                            exc_info=False)
                        continue
                else:
                    logger.warning(
                        f"Skipping row {i+1} with insufficient cells ({len(cells)}) on {index_page_url}"
                    )

            # --- 5. Return Results ---
            if is_likely_abs:
                # If flagged as ABS, return None for filename regardless of whether one was found
                logger.info(
                    f"Filing {accession_number} flagged as likely ABS, skipping primary document selection."
                )
                return None, True
            elif parsed_htm_filename:
                # Found a primary doc and not flagged as ABS
                logger.info(
                    f"Primary document for {accession_number} identified as: {parsed_htm_filename}"
                )
                return parsed_htm_filename, False
            else:
                # Did not find a matching primary doc and not flagged as ABS
                logger.warning(
                    f"No primary document found matching types {target_form_types} for {accession_number}"
                )
                return None, False

        except Exception as parse_err:
            # Catch unexpected errors during BeautifulSoup processing
            logger.error(
                f"Failed to parse HTML content for {index_page_url}: {parse_err}",
                exc_info=True)
            raise ParsingError(f"HTML parsing failed: {parse_err}",
                               source=index_page_url)
