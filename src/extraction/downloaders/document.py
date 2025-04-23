# src/extraction/downloaders/document.py

import logging
import shutil
from pathlib import Path
import requests  # For specific exceptions

# Assuming .base defines AbstractDownloader
from .base import AbstractDownloader
from src.core.exceptions import DownloadError, FileSystemError, NotFoundError, RequestTimeoutError  # Import exceptions

logger = logging.getLogger(__name__)


class DocumentDownloader(AbstractDownloader):
    """
    Downloads specific filing documents (e.g., 10-K HTML files) from SEC EDGAR Archives.
    """

    def _build_document_url(self, cik: str, accession_number: str,
                            filename: str) -> str | None:
        """Constructs the EDGAR URL for a specific document."""
        if not cik or not accession_number or not filename:
            logger.error(
                "Cannot build URL: CIK, accession number, and filename are required."
            )
            return None
        try:
            # Remove leading zeros from CIK for URL construction
            cik_no_zeros = str(int(cik))
        except ValueError:
            logger.error(f"Invalid CIK format for URL construction: {cik}")
            return None
        # Remove dashes from accession number for URL path
        acc_no_dashes = accession_number.replace('-', '')

        # Use edgar_archive_base from sec_api_settings
        url = f"{self.sec_api_settings.edgar_archive_base}/{cik_no_zeros}/{acc_no_dashes}/{filename}"
        return url

    # Overriding download method from base class
    def download(self, cik: str, accession_number: str, filename: str,
                 output_path: Path) -> bool:
        """
        Downloads a specific filing document.

        Args:
            cik: The CIK of the company.
            accession_number: The accession number of the filing (with dashes).
            filename: The specific filename of the document to download (e.g., 'd123456d10k.htm').
            output_path: The local path where the downloaded document should be saved.

        Returns:
            True if download is successful, False otherwise.

        Raises:
            ValueError: If input parameters are invalid for URL construction.
            FileSystemError: If the output directory cannot be created.
            RequestTimeoutError: If the request times out.
            NotFoundError: If the document returns a 404 status.
            DownloadError: For other HTTP errors or network issues.
        """
        url = self._build_document_url(cik, accession_number, filename)
        if not url:
            # Error logged in helper, raise exception for clarity
            raise ValueError(
                f"Could not construct valid document URL for CIK={cik}, AccNo={accession_number}, File={filename}"
            )

        # Ensure output directory exists
        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(
                f"Failed to create directory {output_path.parent}: {e}",
                exc_info=True)
            raise FileSystemError(
                f"Cannot create output directory: {output_path.parent}")

        logger.info(
            f"Attempting to download document from {url} to {output_path}")

        try:
            # Use base class helper, enable streaming for potentially large HTML/XBRL files
            # Use default SEC headers. Standard timeout should be sufficient.
            response = self._make_request(
                url, headers=self.headers, stream=True,
                timeout=120)  # Slightly longer timeout

            # Stream the download to the file
            with open(output_path, 'wb') as f_out:
                shutil.copyfileobj(response.raw, f_out,
                                   length=1024 * 1024)  # 1MB chunks

            # Optional: Add Content-Length check here too if desired
            # file_size = output_path.stat().st_size
            # content_length = response.headers.get('content-length')
            # if content_length and int(content_length) != file_size:
            #     logger.warning(...) # Handle mismatch

            logger.info(f"Successfully downloaded document to {output_path}")
            return True

        # Catch specific exceptions raised by _make_request or file operations
        except NotFoundError:
            logger.warning(f"Document not found at {url} (404)"
                           )  # Log as warning, could be expected
            return False
        except RequestTimeoutError:
            logger.error(f"Timeout downloading document from {url}")
            return False  # Or re-raise if timeout should halt the process? Return False for now.
        except DownloadError as e:
            # Catch other network/HTTP errors from _make_request
            logger.error(f"Download failed for document {url}: {e}",
                         exc_info=True)
            return False  # Or re-raise
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
                f"An unexpected error occurred during document download: {e}",
                exc_info=True)
            # Clean up partial file
            try:
                if output_path.exists(): output_path.unlink()
            except OSError:
                pass
            return False  # Or re-raise
        finally:
            # Clean up response object if needed (handled by requests usually)
            pass
