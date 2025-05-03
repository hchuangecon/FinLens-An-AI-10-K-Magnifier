# src/phase2_parsing/extractors/metadata_extractor.py
import logging
import re
from typing import Optional, Dict, Any, List, TYPE_CHECKING
from datetime import datetime, date

# --- sec-parser Imports ---
# Import base class and specific element types known to contain metadata
try:
    from sec_parser.semantic_elements.abstract_semantic_element import AbstractSemanticElement
    # --- Corrected Import Path ---
    # Import specific elements directly from sec_parser.semantic_elements
    from sec_parser.semantic_elements import (
        TopLevelSectionTitle,
        TextElement,  # Keep TextElement if needed for fallback text collection
        DocumentTypeElement,
        EntityRegistrantNameElement,
        TradingSymbolElement,
        DocumentFiscalYearFocusElement,
        DocumentFiscalPeriodFocusElement,
        CentralIndexKeyElement
        # Add other relevant element types if needed
    )
    # --- End Correction ---
    SEC_PARSER_AVAILABLE = True
except ImportError as e:
    # Log the specific error to help diagnose
    logging.warning(
        f"sec-parser types not found: {e}. MetadataExtractor may not function correctly."
    )
    SEC_PARSER_AVAILABLE = False

# --- Removed Docling Imports ---

logger = logging.getLogger(__name__)


class MetadataExtractor:
    """
    Extracts specific metadata fields from a list of sec-parser semantic elements
    or falls back to regex on concatenated text.
    """

    def __init__(self):
        """ Initializes the extractor with regex patterns for fallback. """
        # Keep regex patterns as fallbacks or for fields not covered by specific elements
        self.fiscal_year_pattern = re.compile(
            # Broader pattern to capture various date formats after the phrase
            r"(?:fiscal\s+(?:year|period)\s+(?:end|ended)|period\s+ended)\s*:?\s*"
            r"([A-Za-z]+\s+\d{1,2},\s+\d{4}|\d{1,2}/\d{1,2}/\d{4}|[A-Za-z]+\s+\d{1,2}\s*\d{4}|[A-Za-z]+\s+\d{4}|December\s+31|September\s+30|June\s+30|March\s+31)"
            r"(?:\s+\d{4})?",  # Optional year again
            re.IGNORECASE)
        # Example pattern for CIK (often found near registrant name)
        self.cik_pattern = re.compile(
            r"\(Central\s+Index\s+Key\s*No\.\s*([0-9-]+)\)", re.IGNORECASE)
        # Add other patterns as needed (e.g., for company name if EntityRegistrantNameElement fails)
        self.company_name_pattern = re.compile(
            r"Exact\s+name\s+of\s+registrant\s+as\s+specified\s+in\s+its\s+charter\s*\n+([^\n]+)",
            re.IGNORECASE)

        logger.info(
            "MetadataExtractor initialized with pre-compiled Regex patterns.")

    def extract(
        self,
        semantic_elements: List[AbstractSemanticElement],
        raw_html_content: Optional[
            str] = None  # Keep raw_html for potential future use or other extractors
    ) -> Dict[str, Any]:
        """
        Extracts metadata by primarily checking specific sec-parser element types,
        then falling back to regex on concatenated text.

        Args:
            semantic_elements: List of semantic elements from sec-parser.
            raw_html_content: Optional raw HTML string (currently unused here).

        Returns:
            Dictionary containing extracted metadata.
        """
        metadata: Dict[str, Any] = {
            'company_name': None,
            'cik': None,
            'document_type': None,
            'fiscal_year_focus': None,
            'fiscal_period_focus': None,
            'fiscal_year_end_date': None,  # Extracted/parsed date
            'fiscal_year_end_date_text': None  # Raw text found
        }
        full_text_for_regex = []  # Collect text for regex fallback

        if not SEC_PARSER_AVAILABLE:
            logger.error(
                "sec-parser elements not available, cannot perform metadata extraction."
            )
            return metadata  # Return empty if parser failed

        if not semantic_elements:
            logger.warning(
                "Received empty list of semantic elements. Cannot extract metadata."
            )
            return metadata

        logger.debug(
            f"Starting metadata extraction from {len(semantic_elements)} semantic elements."
        )

        # --- Primary Extraction: Iterate through specific sec-parser elements ---
        for element in semantic_elements:
            # Extract text for fallback regex regardless of type
            # Use get_text() method which might handle composite elements better
            if hasattr(element, 'get_text'):
                full_text_for_regex.append(element.get_text())
            elif hasattr(element, 'text'):
                full_text_for_regex.append(element.text)

            # Check for specific metadata element types
            if isinstance(element, EntityRegistrantNameElement
                          ) and not metadata.get('company_name'):
                metadata['company_name'] = element.text.strip()
                logger.debug(f"Found Company Name: {metadata['company_name']}")
                # Look for CIK nearby (common pattern)
                if not metadata.get('cik'):
                    cik_match = self.cik_pattern.search(element.text)
                    if cik_match:
                        metadata['cik'] = cik_match.group(1).strip().replace(
                            '-', '')
                        logger.debug(
                            f"Found CIK near Company Name: {metadata['cik']}")

            elif isinstance(
                    element,
                    CentralIndexKeyElement) and not metadata.get('cik'):
                metadata['cik'] = element.text.strip().replace('-', '')
                logger.debug(f"Found CIK Element: {metadata['cik']}")

            elif isinstance(
                    element,
                    DocumentTypeElement) and not metadata.get('document_type'):
                metadata['document_type'] = element.text.strip().upper()
                logger.debug(
                    f"Found Document Type: {metadata['document_type']}")

            elif isinstance(element, DocumentFiscalYearFocusElement
                            ) and not metadata.get('fiscal_year_focus'):
                metadata['fiscal_year_focus'] = element.text.strip()
                logger.debug(
                    f"Found Fiscal Year Focus: {metadata['fiscal_year_focus']}"
                )

            elif isinstance(element, DocumentFiscalPeriodFocusElement
                            ) and not metadata.get('fiscal_period_focus'):
                metadata['fiscal_period_focus'] = element.text.strip().upper()
                logger.debug(
                    f"Found Fiscal Period Focus: {metadata['fiscal_period_focus']}"
                )
                # Often the fiscal year end date is mentioned here
                if not metadata.get('fiscal_year_end_date'):
                    fy_end_text = self._find_first_match(
                        self.fiscal_year_pattern, element.text)
                    if fy_end_text:
                        metadata['fiscal_year_end_date_text'] = fy_end_text
                        parsed_date = self._parse_date(
                            fy_end_text, metadata.get('fiscal_year_focus')
                        )  # Pass year focus for context
                        if parsed_date:
                            metadata[
                                'fiscal_year_end_date'] = parsed_date.strftime(
                                    '%Y-%m-%d')
                            logger.debug(
                                f"Found Fiscal Year End (from Period Focus): {metadata['fiscal_year_end_date']}"
                            )

            # Example: Check TopLevelSectionTitle (though not typically metadata, just showing usage)
            elif isinstance(element, TopLevelSectionTitle):
                logger.debug(
                    f"Encountered TopLevelSectionTitle: {element.text.strip()}"
                )

            # Stop iterating early if all primary fields are found? Optional optimization.
            primary_keys = [
                'company_name', 'cik', 'document_type', 'fiscal_year_focus',
                'fiscal_period_focus'
            ]
            if all(metadata.get(k) for k in primary_keys):
                logger.debug(
                    "Found all primary metadata elements, stopping iteration early."
                )
                break

        # --- Fallback Extraction: Use Regex on concatenated text ---
        full_text = "\n".join(full_text_for_regex)

        if not metadata.get('fiscal_year_end_date'):
            fy_end_text = self._find_first_match(self.fiscal_year_pattern,
                                                 full_text)
            if fy_end_text:
                metadata['fiscal_year_end_date_text'] = fy_end_text
                # Pass fiscal year focus if available for context
                parsed_date = self._parse_date(
                    fy_end_text, metadata.get('fiscal_year_focus'))
                if parsed_date:
                    metadata['fiscal_year_end_date'] = parsed_date.strftime(
                        '%Y-%m-%d')
                    logger.debug(
                        f"Found Fiscal Year End (Regex Fallback): {metadata['fiscal_year_end_date']}"
                    )

        if not metadata.get('cik'):
            cik_match_fallback = self.cik_pattern.search(full_text)
            if cik_match_fallback:
                metadata['cik'] = cik_match_fallback.group(1).strip().replace(
                    '-', '')
                logger.debug(f"Found CIK (Regex Fallback): {metadata['cik']}")

        if not metadata.get('company_name'):
            company_match_fallback = self.company_name_pattern.search(
                full_text)
            if company_match_fallback:
                metadata['company_name'] = company_match_fallback.group(
                    1).strip()
                logger.debug(
                    f"Found Company Name (Regex Fallback): {metadata['company_name']}"
                )

        # Clean up None values if desired, or keep them to indicate missing data
        final_metadata = {k: v for k, v in metadata.items() if v is not None}
        # return final_metadata
        logger.info(f"Metadata extraction completed. Found: {final_metadata}")
        return metadata  # Return original dict with Nones for clarity

    def _find_first_match(self, pattern: re.Pattern,
                          text: str) -> Optional[str]:
        """ Uses a pre-compiled regex pattern to find the first matching group in text. """
        if not text: return None
        match = pattern.search(text)
        if match:
            try:
                # Try to get group 1, fallback to group 0 if no capturing group
                return match.group(1).strip()
            except IndexError:
                try:
                    # Special case for month-day only formats captured by group 0
                    if pattern == self.fiscal_year_pattern and re.match(
                            r"([A-Za-z]+\s+\d{1,2})$",
                            match.group(0).strip()):
                        return match.group(0).strip()
                    # Otherwise, log error if group 0 isn't what we expect
                    logger.debug(
                        f"Regex match found but group 1 missing for pattern: {pattern.pattern}. Full match: {match.group(0)}"
                    )
                    return None  # Avoid returning full match unless specifically handled
                except IndexError:
                    logger.error(
                        f"Regex match found but no groups captured for pattern: {pattern.pattern}"
                    )
                    return None
            except Exception as e:
                logger.error(f"Error extracting group from regex match: {e}")
                return None
        return None

    def _parse_date(
            self,
            date_str: Optional[str],
            fiscal_year_context: Optional[str] = None) -> Optional[date]:
        """ Attempts to parse common date formats from a string, using fiscal year for context if needed. """
        if not date_str:
            return None

        cleaned_date_str = date_str.strip().strip(':').strip()
        year_to_use = None

        # Try to extract year from context if available
        if fiscal_year_context:
            year_match = re.search(r'\b(\d{4})\b', fiscal_year_context)
            if year_match:
                year_to_use = int(year_match.group(1))

        # Handle simple month-day cases like "December 31"
        month_day_match = re.match(r"([A-Za-z]+)\s+(\d{1,2})$",
                                   cleaned_date_str)
        if month_day_match and year_to_use:
            month = month_day_match.group(1)
            day = month_day_match.group(2)
            # Try parsing with the context year
            try:
                # Use formats that include month and day
                temp_date_str = f"{month} {day}, {year_to_use}"
                parsed = datetime.strptime(temp_date_str, "%B %d, %Y").date()
                logger.debug(
                    f"Parsed month-day '{cleaned_date_str}' with year {year_to_use} -> {parsed}"
                )
                return parsed
            except ValueError:
                logger.warning(
                    f"Could not parse month-day '{cleaned_date_str}' using context year {year_to_use}"
                )
                # Fall through to try other formats without the assumed year

        # Try standard formats
        formats_to_try = [
            "%B %d, %Y",
            "%b %d, %Y",  # Month Day, Year (e.g., September 30, 2023)
            "%m/%d/%Y",
            "%m-%d-%Y",  # MM/DD/YYYY, MM-DD-YYYY
            # "%B %Y", "%b %Y",       # Month Year - too ambiguous, avoid for now
        ]
        for fmt in formats_to_try:
            try:
                return datetime.strptime(cleaned_date_str, fmt).date()
            except ValueError:
                continue

        logger.warning(
            f"Could not parse date string '{cleaned_date_str}' with known formats."
        )
        return None
