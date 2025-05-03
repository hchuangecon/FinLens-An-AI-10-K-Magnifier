# src/phase2_parsing/parsers/sec_parser_wrapper.py
import logging
from typing import Any, Dict, List, Type
from pathlib import Path
import sys

# --- sec-parser Imports ---
SEC_PARSER_AVAILABLE = False
try:
    # Import both parser types and the base class
    from sec_parser import Edgar10KParser, Edgar10QParser
    from sec_parser.processing_engine import AbstractSemanticElementParser
    from sec_parser.semantic_elements.abstract_semantic_element import AbstractSemanticElement
    SEC_PARSER_AVAILABLE = True
    logging.debug("sec-parser library found for SECParserWrapper.")
except ImportError as e:
    SEC_PARSER_AVAILABLE = False
    logging.critical(
        f"sec-parser library not found or missing expected classes (Edgar10KParser/Edgar10QParser): {e}. Install with: pip install sec-parser"
    )
    raise ImportError(
        "sec-parser library (with Edgar10KParser/Edgar10QParser) is required for SECParserWrapper"
    )
# --------------------------

# --- Corrected Relative Import for the Interface ---
try:
    from ..types.interfaces import AbstractParser
except ImportError:
    logging.critical(
        "Could not import AbstractParser from ..types.interfaces - check structure."
    )
    raise
# --------------------------------------------------

logger = logging.getLogger(__name__)


class SECParserWrapper(AbstractParser):
    """
    Concrete implementation of AbstractParser using the sec-parser library.
    Selects the appropriate parser (10-K or 10-Q) based on document metadata.
    """

    def __init__(self):
        if not SEC_PARSER_AVAILABLE:
            raise ImportError(
                "sec-parser library is required but was not loaded.")
        # No parser instantiated here anymore
        logger.info(f"Initialized {self.__class__.__name__}.")

    def parse(self, html_content: str,
              doc_metadata: Dict[str, Any]) -> List[AbstractSemanticElement]:
        """
        Parses HTML content using sec-parser, selecting the parser based on form_type.

        Args:
            html_content: The raw HTML string.
            doc_metadata: Document metadata, must contain 'form_type'.

        Returns:
            A list of sec_parser AbstractSemanticElement objects.

        Raises:
            ValueError: If 'form_type' is missing in doc_metadata or unsupported.
            Exception: If sec-parser fails during parsing (re-raised).
        """
        identifier = doc_metadata.get(
            "filename_base", doc_metadata.get("accession_number",
                                              "unknown_doc"))
        form_type = doc_metadata.get("form_type", "").upper()

        parser_class: Type[
            AbstractSemanticElementParser]  # Type hint for the chosen class

        if form_type.startswith("10-K"):
            parser_class = Edgar10KParser
            logger.info(
                f"Selecting Edgar10KParser for {identifier} (form_type: {form_type})"
            )
        elif form_type.startswith("10-Q"):
            parser_class = Edgar10QParser
            logger.info(
                f"Selecting Edgar10QParser for {identifier} (form_type: {form_type})"
            )
        else:
            # Fallback or Error? Let's raise an error for now.
            # Could default to Edgar10KParser if preferred.
            logger.error(
                f"Unsupported or missing form_type '{form_type}' for {identifier}"
            )
            raise ValueError(
                f"Unsupported or missing form_type '{form_type}' for sec-parser selection."
            )

        logger.info(f"Running {parser_class.__name__} on {identifier}...")
        try:
            # Instantiate the selected parser class
            parser_instance = parser_class()
            # Pass the HTML string content to the instance's parse method
            semantic_elements: List[
                AbstractSemanticElement] = parser_instance.parse(html_content)
            logger.info(
                f"{parser_class.__name__} returned {len(semantic_elements)} elements for {identifier}."
            )
            return semantic_elements
        except Exception as e:
            logger.error(
                f"sec-parser ({parser_class.__name__}) failed processing {identifier}: {e}",
                exc_info=True)
            # Re-raise the exception so the ProcessorService can handle it
            raise
