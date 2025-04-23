# src/config/logging_config.py
import logging.config
import os

LOG_LEVEL = os.getenv(
    "LOG_LEVEL", "INFO").upper()  # Default to INFO, allow override via env var
LOG_FILE = "logs/pipeline_run.log"  # Or get from settings

# Ensure the logs directory exists (optional)
# log_dir = Path("logs")
# log_dir.mkdir(exist_ok=True)
# log_file_path = log_dir / LOG_FILE

LOGGING_CONFIG = {
    "version": 1,
    "disable_existing_loggers":
    False,  # Keep default loggers (like sqlalchemy) unless needed
    "formatters": {
        "standard": {
            "format":
            "%(asctime)s - %(name)s:%(lineno)d - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "simple": {
            "format": "%(levelname)s - %(message)s",
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": LOG_LEVEL,  # Console level controlled by env var
            "formatter": "standard",
            "stream": "ext://sys.stdout",  # Use stdout
        },
        "file": {
            "class":
            "logging.handlers.RotatingFileHandler",  # Example: rotate logs
            "level": "DEBUG",  # Log DEBUG level and up to file
            "formatter": "standard",
            "filename": LOG_FILE,  # Use constant or path from settings
            "maxBytes": 10485760,  # 10MB
            "backupCount": 5,  # Keep 5 backup logs
            "encoding": "utf8",
        },
        # Add other handlers if needed (e.g., SysLogHandler, HTTPHandler)
    },
    "loggers": {
        # Root logger: catches everything not handled by specific loggers
        "": {
            "handlers": ["console", "file"],  # Send to both console and file
            "level": "INFO",
            "propagate": False,  # Prevent root messages going to parent (none)
        },
        # Specific logger level examples:
        "sqlalchemy.engine": {
            "handlers": ["file"],  # Send SQL logs only to file
            "level": "WARNING",  # Only log SQL warnings/errors, not INFO/DEBUG
            "propagate":
            False,  # Don't send SQL logs to root logger's handlers
        },
        "urllib3": {
            "level": "WARNING",
            "propagate": True,  # Let root handle it
        },
        "asyncio":
        {  # If using asyncio later
            "level": "WARNING",
            "propagate": True,
        },
        "src.extraction.parsers.json": {
            "level": "WARNING",
            "propagate": True
        },
        # Add levels for specific modules if needed:
        # "src.extraction.downloaders": {
        #     "level": "INFO",
        #     "propagate": True,
        # }
    },
}


def setup_logging():
    """Applies the logging configuration."""
    logging.config.dictConfig(LOGGING_CONFIG)
    logger = logging.getLogger(__name__)
    logger.info(
        f"Logging configured. Console Level: {LOG_LEVEL}, File Level: DEBUG")
