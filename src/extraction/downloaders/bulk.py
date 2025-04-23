# src/extraction/downloaders/bulk.py

import logging
import shutil  # For efficient file copying from stream
from pathlib import Path
import requests  # Although _make_request uses it, we need it for exception types

# Assuming .base defines AbstractDownloader
from .base import AbstractDownloader
from src.core.exceptions import DownloadError, FileSystemError, NotFoundError, RequestTimeoutError  # Import exceptions

logger = logging.getLogger(__name__)


class BulkDownloader(AbstractDownloader):
    """
    Downloads the bulk SEC submissions file (submissions.zip).
    """

    def download(self,
                 url: str | None = None,
                 output_path: Path | None = None) -> bool:
        """
        Downloads the bulk submissions file from the specified URL.

        Args:
            url: The URL of the bulk submissions file. If None, uses the default
                 from SEC API settings.
            output_path: The local path where the downloaded zip file should be saved.
                         If None, uses default based on pipeline settings (e.g., data_path / "submissions.zip").

        Returns:
            True if download is successful, False otherwise.
        """
        if url is None:
            url = self.sec_api_settings.submissions_bulk_url
            logger.debug(f"Using default bulk submissions URL: {url}")
        if output_path is None:
            output_path = self.settings.pipeline.data_path / "submissions.zip"
            logger.debug(f"Using default output path: {output_path}")

        # Ensure output directory exists
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(
                f"Failed to create directory {output_path.parent}: {e}",
                exc_info=True)
            # Raise a specific FileSystemError
            raise FileSystemError(
                f"Cannot create output directory: {output_path.parent}")

        logger.info(
            f"Attempting to download bulk file from {url} to {output_path}")

        try:
            # Use the base class helper to make the request, enabling streaming
            # Use the default SEC headers (Host: www.sec.gov)
            response = self._make_request(
                url, headers=self.headers, stream=True,
                timeout=900)  # Increased timeout for large file

            # Stream the download to the file efficiently
            with open(output_path, 'wb') as f_out:
                # Use shutil.copyfileobj for potentially better memory efficiency
                shutil.copyfileobj(response.raw, f_out, length=1024 *
                                   1024)  # Read/write in 1MB chunks

            # Verify download size
            file_size = output_path.stat().st_size
            content_length = response.headers.get('content-length')
            if content_length and int(content_length) != file_size:
                logger.warning(
                    f"Downloaded file size ({file_size}) does not match Content-Length ({content_length}) for {url}"
                )
                # Decide if this is an error or just a warning

            logger.info(f"Successfully downloaded bulk file to {output_path}")
            return True

        # Catch specific exceptions raised by _make_request
        except NotFoundError:
            logger.error(f"Bulk file not found at {url} (404)")
            return False
        except RequestTimeoutError:
            logger.error(f"Timeout downloading bulk file from {url}")
            return False
        except DownloadError as e:
            # Catch other network/HTTP errors from _make_request
            logger.error(f"Download failed for bulk file {url}: {e}",
                         exc_info=True)
            return False
        except IOError as e:
            # Catch errors during file writing
            logger.error(
                f"Failed to write downloaded data to {output_path}: {e}",
                exc_info=True)
            # Attempt to clean up partial download
            try:
                if output_path.exists(): output_path.unlink()
            except OSError:
                logger.warning(
                    f"Could not remove partially downloaded file: {output_path}"
                )
            return False
        except Exception as e:
            # Catch any other unexpected errors
            logger.error(
                f"An unexpected error occurred during bulk download: {e}",
                exc_info=True)
            return False
        finally:
            # Ensure response object is closed if it exists (requests should handle this with context manager in _make_request if possible)
            # but good practice if we handled response directly. _make_request doesn't use context manager yet.
            # if 'response' in locals() and response: response.close() # Might be needed if not using context manager
            pass  # _make_request doesn't guarantee response closure, but usually ok.
