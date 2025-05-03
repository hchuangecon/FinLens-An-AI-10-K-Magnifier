# src/core/exceptions.py


class FinlensError(Exception):
    """Base class for all custom exceptions in the Finlens project."""
    pass


# --- Configuration Errors ---
class ConfigurationError(FinlensError):
    """Error related to application configuration."""
    pass


# --- Database Errors ---
class DatabaseError(FinlensError):
    """Error related to database operations."""
    pass


class DatabaseConnectionError(DatabaseError):
    """Error establishing a connection to the database."""
    pass


class DatabaseQueryError(DatabaseError):
    """Error during the execution of a database query."""
    pass


class DatabaseIntegrityError(DatabaseError):
    """Error related to database integrity constraints (e.g., duplicate keys)."""
    # Often wraps sqlalchemy.exc.IntegrityError
    pass


# --- Network/Download Errors ---
class NetworkError(FinlensError):
    """Error related to network operations (e.g., downloading from SEC)."""

    def __init__(self,
                 message: str,
                 url: str | None = None,
                 status_code: int | None = None):
        self.url = url
        self.status_code = status_code
        full_message = f"{message}"
        if url:
            full_message += f" | URL: {url}"
        if status_code:
            full_message += f" | Status Code: {status_code}"
        super().__init__(full_message)


class DownloadError(NetworkError):
    """Specific error during file download."""
    pass


class RequestTimeoutError(NetworkError):
    """Network request timed out."""
    pass


class NotFoundError(NetworkError):
    """Resource not found (e.g., HTTP 404)."""

    def __init__(self,
                 message: str = "Resource not found",
                 url: str | None = None):
        super().__init__(message, url=url, status_code=404)


# --- Parsing Errors ---
class ParsingError(FinlensError):
    """Error related to parsing data (e.g., JSON, HTML, Index files)."""

    def __init__(self, message: str, source: str | None = None):
        self.source = source  # e.g., filename, URL
        full_message = f"{message}"
        if source:
            full_message += f" | Source: {source}"
        super().__init__(full_message)


class JSONParsingError(ParsingError):
    """Error parsing JSON data."""
    pass


class IndexParsingError(ParsingError):
    """Error parsing SEC index files."""
    pass


class HTMLParsingError(ParsingError):
    """Error parsing HTML content."""
    pass


# --- File System Errors ---
class FileSystemError(FinlensError):
    """Error related to file system operations."""
    pass


# --- Pipeline Logic Errors ---
class PipelineError(FinlensError):
    """Error related to the execution logic of a pipeline step."""
    pass
