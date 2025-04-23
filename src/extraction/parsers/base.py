# finlens/extraction/parsers/base.py

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any  # Use Any for now, can be refined later (e.g., Tuple, Dict)
from src.config.settings import AppSettings

logger = logging.getLogger(__name__)


class AbstractParser(ABC):
    """
    Abstract Base Class for all data parsers handling SEC formats.
    """

    def __init__(self, settings: AppSettings):
        """
        Initializes the parser with necessary configurations.

        Args:
            settings: The application settings object.
        """
        self.settings = settings
        # Parsers might not need API settings directly, but maybe file paths from PipelineSettings
        self.pipeline_settings = settings.pipeline
        logger.info(f"{self.__class__.__name__} initialized.")

    @abstractmethod
    def parse(self, input_source: Any, *args, **kwargs) -> Any:
        """
        Primary method to perform the parsing operation.

        Args:
            input_source: The data source to parse (e.g., file path, raw content, URL).
                          The type will depend on the specific parser.
            *args, **kwargs: Additional arguments specific to the parser.

        Returns:
            The parsed data in a structured format (e.g., list of dicts, custom objects).
            The exact return type will depend on the parser. Returns None or raises
            ParsingError on failure.
        """
        pass

    # Common helper methods for parsing could be added here if identified later.
    # For example, common text cleaning, date parsing utilities, etc.
