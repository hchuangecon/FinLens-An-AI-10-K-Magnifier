# src/phase2_parsing/extractors/toc_extractor.py
"""
Table of Contents Extractor using BeautifulSoup on raw HTML.
Uses the specific regex provided by the user and attempts parsing
with 'html.parser', falling back to 'lxml' if the initial parse fails.
"""
import logging
import re
import warnings
from typing import Any, Dict, List, Tuple, Optional

# --- Try importing lxml ---
LXML_AVAILABLE = False
try:
    import lxml
    LXML_AVAILABLE = True
except ImportError:
    pass  # lxml is optional

# --- BeautifulSoup Import ---
# Import specific exceptions if needed for finer control,
# but a general Exception might suffice for parser failures.
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning

# --- Setup Logging ---
logger = logging.getLogger(__name__)

# Suppress XML-as-HTML warnings from BeautifulSoup
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)


# --- Utilities ---
def sanitize_for_section_id(text: str) -> str:
    """Basic sanitization for creating section IDs from titles."""
    if not text:
        return "unknown_section"
    # Extract the core identifier (PART_I, ITEM_1A etc.) using the start of the user's regex pattern
    match = re.match(r'(?i)^\s*(ITEM\s+\d+[A-Z]?|PART\s+[IVXLC]+)', text)
    if match:
        sanitized = re.sub(r"[^a-z0-9]", "",
                           match.group(1).lower().replace(' ', '_'))
        # Ensure item_1a format
        sanitized = re.sub(r'item(\d+)([a-z])', r'item_\1\2', sanitized)
        return sanitized or "sanitized_empty"  # Fallback if sanitization results in empty string

    # Fallback for non-standard matches (less likely needed with ITEM_LINE_RE but safe)
    sanitized = re.sub(r"[^a-z0-9 \-]", "", text.lower())
    sanitized = re.sub(r"[ \-]+", "_", sanitized).strip('_')
    return sanitized or "sanitized_empty"


class ToCExtractor:
    """
    Extracts a table of contents by scanning raw HTML text lines, anchors,
    and common block tags for "ITEM X" and "PART I" headings using BeautifulSoup.
    Uses the user-provided regex and attempts 'html.parser', falling back to 'lxml'.
    """
    # Using the exact regex provided by the user
    ITEM_LINE_RE = re.compile(
        r'''(?xi)               # ignore case, verbose
        ^\s*(PART\s+[IVX]+|ITEM\s+\d+[A-Z]?)   # “PART I” or “ITEM 1A” (Group 1)
        [\.\-\—:\s]* # separator
        (?P<title>[\w\(\)&/\,\-\s]+?)            # section title (named group 'title')
        # Optional non-capturing group for page number indicators
        (?:\s*(?:\.{2,}|\s{2,}|–|—)\s*(?P<page>\d{1,3})?\s*)?
        $''', re.MULTILINE | re.VERBOSE)

    def _get_level_from_match(self, match_group_1: str) -> int:
        """Determines semantic level (1 for PART, 2 for ITEM #, 3 for ITEM #A)."""
        identifier = match_group_1.upper().strip()
        if identifier.startswith("PART"): return 1
        elif identifier.startswith("ITEM"):
            return 3 if identifier[-1].isalpha() else 2
        return 2  # Default

    def extract_from_html(self, html: str) -> List[Tuple[str, int, str]]:
        """
        Parses raw HTML with BeautifulSoup, trying 'html.parser' first,
        then 'lxml' as a fallback, and extracts potential TOC entries.
        """
        candidates: List[Dict[str, Any]] = []
        if not html:
            logger.warning("No HTML content provided for TOC extraction.")
            return []

        soup: Optional[BeautifulSoup] = None
        parser_used: Optional[str] = None

        # --- Attempt 1: html.parser ---
        try:
            logger.debug("Attempting to parse HTML using 'html.parser'...")
            soup = BeautifulSoup(html, 'html.parser')
            parser_used = 'html.parser'
            logger.debug("Successfully parsed HTML using 'html.parser'.")
        except Exception as e_html:
            logger.warning(
                f"BeautifulSoup parsing with 'html.parser' failed: {e_html}. Trying 'lxml'..."
            )
            # --- Attempt 2: lxml (Fallback) ---
            if LXML_AVAILABLE:
                try:
                    soup = BeautifulSoup(html, 'lxml')
                    parser_used = 'lxml'
                    logger.info(
                        "Successfully parsed HTML using fallback 'lxml'."
                    )  # Log success with lxml
                except Exception as e_lxml:
                    logger.error(
                        f"BeautifulSoup parsing failed with both 'html.parser' and 'lxml'. LXML error: {e_lxml}"
                    )
                    return []  # Return empty if both fail
            else:
                logger.error(
                    "BeautifulSoup parsing failed with 'html.parser' and 'lxml' is not installed. Cannot extract ToC."
                )
                return [
                ]  # Return empty if html.parser fails and lxml not available

        # --- Proceed with extraction if soup object was created ---
        if soup is None:
            logger.error("HTML could not be parsed by any available parser.")
            return []

        processed_texts = set()

        # Function to process a potential match
        def process_match(m, offset):
            if m:
                identifier_group = m.group(1)
                title_group = m.group('title')
                if identifier_group and title_group:
                    level = self._get_level_from_match(identifier_group)
                    title = title_group.strip()
                    if not title: return False  # Skip empty titles
                    sec_id = sanitize_for_section_id(identifier_group)
                    # Check if this exact combination already exists to avoid duplicates from different methods
                    # Using a tuple of key elements as the check
                    candidate_key = (sec_id, title, level)
                    # Simple check based on key, could be refined if needed
                    if candidate_key not in [(c['id'], c['title'], c['level'])
                                             for c in candidates]:
                        candidates.append({
                            'id': sec_id,
                            'title': title,
                            'level': level,
                            'offset': offset
                        })
                        return True
            return False

        # 1) Scan rendered text lines
        try:
            all_text = soup.get_text(separator='\n')
            for idx, line in enumerate(all_text.splitlines()):
                line = line.strip()
                if not line or line in processed_texts: continue
                match = self.ITEM_LINE_RE.search(line)  # Use search
                if process_match(match, idx):
                    processed_texts.add(
                        line
                    )  # Add raw line to prevent re-processing same text block
        except Exception as e:
            logger.warning(f"Error processing soup.get_text() lines: {e}")

        # 2) Scan common block tags
        try:
            tags_to_scan = [
                'td', 'th', 'p', 'div', 'span', 'li', 'a', 'b', 'strong',
                'font', 'h1', 'h2', 'h3', 'h4'
            ]
            for tag in soup.find_all(tags_to_scan):
                # Get text only from the direct tag, avoiding nested tags repeating text
                # Use find_all(text=True, recursive=False) and join
                direct_text_parts = tag.find_all(text=True, recursive=False)
                text = ' '.join(part.strip()
                                for part in direct_text_parts).strip()

                # Fallback: If no direct text, get text from immediate children only
                if not text and tag.contents:
                    text = ' '.join(
                        str(c).strip() for c in tag.contents
                        if isinstance(c, str)).strip()

                # Limit length check
                text = text[:500]

                if not text or text in processed_texts: continue

                offset = getattr(tag, 'sourceline',
                                 float('inf'))  # Use inf as default offset
                match = self.ITEM_LINE_RE.search(text)  # Use search
                if process_match(match, offset):
                    processed_texts.add(text)  # Add unique text content found
        except Exception as e:
            logger.warning(f"Error processing block tags: {e}")

        # 3) De-duplicate by id, keeping the earliest offset
        best: Dict[str, Dict[str, Any]] = {}
        for c in candidates:
            # Ensure essential keys exist
            if not c.get('id') or not c.get('title') or 'level' not in c:
                logger.debug(
                    f"Skipping candidate due to missing key fields: {c}")
                continue

            current_id = c['id']
            current_offset = c.get(
                'offset', float('inf'))  # Handle potential missing offset

            if current_id not in best or current_offset < best[current_id].get(
                    'offset', float('inf')):
                best[current_id] = c
            # Refined tie-breaking: if same offset, prefer shorter title (less likely to have page numbers etc.)
            elif current_offset == best[current_id].get(
                    'offset', float('inf')):
                if len(c['title']) < len(best[current_id].get('title', '')):
                    best[current_id] = c

        # 4) Convert to final list and sort by original offset
        toc_list = sorted([(d['title'], d['level'], d['id'])
                           for d in best.values()],
                          key=lambda x: best[x[2]].get('offset', float('inf'))
                          )  # Sort using offset from 'best' dict

        logger.info(
            f"Extracted {len(toc_list)} unique TOC entries using BeautifulSoup ('{parser_used}')."
        )
        if not toc_list:
            logger.warning(
                f"No TOC entries extracted using BeautifulSoup ('{parser_used}')."
            )
        return toc_list
