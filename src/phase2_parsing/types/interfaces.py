# src/phase2_parsing/types/interfaces.py
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

# --- Import FinLensNode from its new location ---
try:
    # Corrected import path relative to this file within the 'types' directory
    from .models import FinLensNode
except ImportError:
    # This fallback might still be useful during initial setup phases
    # if models.py hasn't been created/saved yet in the correct location.
    logging.error(
        "Could not import FinLensNode from .models - check file location and __init__.py in 'types' folder"
    )

logger = logging.getLogger(__name__)


class AbstractParser(ABC):
    """
    Interface for parsing raw document content (e.g., HTML) into an
    intermediate representation suitable for node building.
    """

    @abstractmethod
    def parse(self, html_content: str, doc_metadata: Dict[str, Any]) -> Any:
        """
        Parses the HTML content.

        Args:
            html_content: The raw HTML string of the document.
            doc_metadata: Dictionary containing metadata about the document
                          (e.g., accession_number, cik, form_type).

        Returns:
            A representation of the parsed document structure (e.g., a list of
            semantic elements, a DoclingDocument). The specific type depends
            on the concrete implementation but should be consumable by the
            corresponding AbstractNodeBuilder.
        """
        pass

    def __repr__(self) -> str:
        # Provides helpful string representation (e.g., <SECParserWrapper>)
        return f"<{self.__class__.__name__}>"


class AbstractNodeBuilder(ABC):
    """
    Interface for building the final FinLensNode tree from the output
    of an AbstractParser.
    """

    @abstractmethod
    def build_tree(
        self, parser_output: Any, doc_metadata: Dict[str, Any]
    ) -> Tuple[List[FinLensNode], Optional[FinLensNode]]:
        """
        Builds the hierarchical list of FinLensNode objects.

        Args:
            parser_output: The output from the corresponding AbstractParser's
                           `parse` method.
            doc_metadata: Dictionary containing metadata about the document,
                          potentially updated by the parser or other steps.

        Returns:
            A tuple containing:
            - A list of all FinLensNode objects created for the document.
            - The root FinLensNode object, or None if creation failed.
        """
        pass

    def __repr__(self) -> str:
        # Provides helpful string representation (e.g., <SecParserNodeBuilder>)
        return f"<{self.__class__.__name__}>"
