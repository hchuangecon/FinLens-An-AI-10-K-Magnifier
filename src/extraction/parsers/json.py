# src/extraction/parsers/json.py

import logging
import json
import re
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# Assuming .base defines AbstractParser
from .base import AbstractParser
from src.core.exceptions import ParsingError, JSONParsingError  # Import custom exceptions

logger = logging.getLogger(__name__)

# Define a type alias for the return structure for clarity
# Tuple[Optional[CompanyDict], List[FilingDict], ErrorCount]
ParseResult = Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]], int]


class JSONParser(AbstractParser):
    """
    Parses CIK JSON submission files obtained from the SEC bulk submissions.zip.
    Extracts company metadata and recent filing information.
    """

    # Overriding the parse method from the base class
    def parse(self, input_source: Path, *args, **kwargs) -> ParseResult:
        """
        Parses a single CIK JSON file.

        Args:
            input_source: The Path object pointing to the CIK*.json file.

        Returns:
            A tuple containing:
            - A dictionary with extracted company data (or None if parsing fails).
            - A list of dictionaries, each representing a recent filing.
            - An integer count of parsing errors encountered within the file.
        """
        file_path = input_source  # Rename for clarity within the method
        company_data: Optional[Dict[str, Any]] = None
        filings_data: List[Dict[str, Any]] = []
        parse_errors: int = 0

        # Extract CIK from filename
        cik_match = re.match(r'CIK(\d{10})\.json', file_path.name,
                             re.IGNORECASE)
        if not cik_match:
            logger.error(
                f"Could not extract CIK from filename: {file_path.name}")
            # Raise an error or return empty? Let's raise for this fundamental issue.
            raise ParsingError(f"Invalid filename format, cannot extract CIK.",
                               source=str(file_path))

        cik = cik_match.group(1)  # cik identifierelse if(){}

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                raw_data = json.load(f)

            # --- Extract Company Info ---
            # Use .get() with defaults to handle potentially missing keys gracefully
            company_name = raw_data.get('entityName', '') or raw_data.get(
                'name', '') or f"Company CIK {cik}"
            addresses = raw_data.get('addresses', {})
            # Ensure addresses is dict before accessing business key
            business_address = addresses.get('business', {}) if isinstance(
                addresses, dict) else {}

            # Convert 0/1 insider flags to Boolean or None
            owner_exists_raw = raw_data.get('insiderTransactionForOwnerExists')
            issuer_exists_raw = raw_data.get(
                'insiderTransactionForIssuerExists')
            owner_exists = bool(owner_exists_raw) if owner_exists_raw in [
                0, 1
            ] else None
            issuer_exists = bool(issuer_exists_raw) if issuer_exists_raw in [
                0, 1
            ] else None

            company_data = {
                'cik':
                cik,
                'name':
                company_name.strip(),
                'sic':
                raw_data.get('sic'),
                'entity_type':
                raw_data.get('entityType'),
                'sic_description':
                raw_data.get('sicDescription'),
                # Match field names from Company model definition
                'insider_trade_owner':
                owner_exists,
                'insider_trade_issuer':
                issuer_exists,
                'phone':
                raw_data.get('phone'),
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
                business_address.get(
                    'zipCode'),  # Note: key is zipCode in JSON
            }
            # --- End Company Info Extraction ---

            # --- Filings Extraction ---
            # Check structure carefully before accessing nested elements
            if 'filings' in raw_data and isinstance(raw_data.get('filings'), dict) and \
               'recent' in raw_data['filings'] and isinstance(raw_data['filings'].get('recent'), dict):

                recent = raw_data['filings']['recent']
                # Ensure all required lists exist and are indeed lists
                required_keys = [
                    'form', 'filingDate', 'accessionNumber', 'primaryDocument'
                ]
                if all(key in recent and isinstance(recent[key], list)
                       for key in required_keys):

                    # Determine the minimum length across all relevant lists to avoid index errors
                    try:
                        # Use list comprehension with check for None/empty list before min()
                        valid_lists = [
                            recent[key] for key in required_keys if recent[key]
                        ]
                        min_len = min(
                            len(lst)
                            for lst in valid_lists) if valid_lists else 0
                    except TypeError:  # Handles case where a value might not be a list unexpectedly
                        min_len = 0
                        parse_errors += 1
                        logger.warning(
                            f"Non-list found in recent filings keys for {cik}",
                            exc_info=True)

                    if min_len > 0:
                        # Iterate safely up to the minimum length
                        for i in range(min_len):
                            try:
                                # Safely access elements using index `i`
                                acc_num = recent['accessionNumber'][i]
                                filename = recent['primaryDocument'][i]
                                form = recent['form'][i]
                                date_str = recent['filingDate'][i]

                                # Basic validation of extracted data
                                if not all([acc_num, filename, form, date_str
                                            ]):
                                    logger.debug(
                                        f"Missing data in filing record index {i} for CIK {cik}"
                                    )
                                    parse_errors += 1
                                    continue  # Skip this filing record

                                # Parse date string
                                try:
                                    filing_date_obj = datetime.strptime(
                                        date_str, '%Y-%m-%d').date()
                                except ValueError:
                                    logger.warning(
                                        f"Invalid date format '{date_str}' in filing record index {i} for CIK {cik}"
                                    )
                                    parse_errors += 1
                                    continue  # Skip this filing record

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
                            except IndexError:
                                # Should not happen with min_len check, but as safety fallback
                                logger.error(
                                    f"IndexError accessing recent filings data for CIK {cik} at index {i}",
                                    exc_info=True)
                                parse_errors += 1
                                continue  # Stop processing this file's filings after index error
                            except Exception as inner_e:
                                # Catch unexpected errors processing a single filing record
                                logger.warning(
                                    f"Error processing filing record index {i} for CIK {cik}: {inner_e}",
                                    exc_info=False)
                                parse_errors += 1
                                continue  # Skip this filing record
                else:
                    logger.debug(
                        f"Missing or non-list keys in recent filings for CIK {cik}"
                    )
                    # Optional: Increment parse_errors if this state is considered an error
                    # parse_errors += 1
            # --- End Filings Extraction ---

        except FileNotFoundError:
            logger.error(f"JSON file not found: {file_path}")
            # Let caller handle FileNotFoundError or re-raise as ParsingError
            raise ParsingError(f"File not found", source=str(file_path))
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {file_path}: {e}")
            # Raise specific JSON error, including source path
            raise JSONParsingError(f"Invalid JSON format: {e}",
                                   source=str(file_path))
        except Exception as e:
            # Catch any other unexpected error during file processing/parsing
            logger.error(f"Failed to parse JSON file {file_path}: {e}",
                         exc_info=True)
            parse_errors += 1  # Increment error count for general failures
            # Return gracefully with partial data if possible, or raise?
            # Let's return partial data and error count. Modify return below if raising is preferred.
            # raise ParsingError(f"Unexpected error parsing file: {e}", source=str(file_path))

        if parse_errors > 0:
            logger.warning(
                f"Encountered {parse_errors} errors while parsing {file_path.name}"
            )

        # Return the extracted data and error count
        return company_data, filings_data, parse_errors
