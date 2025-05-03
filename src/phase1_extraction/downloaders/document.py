# src/phase1_extraction/downloaders/document.py

import logging
import shutil
from pathlib import Path
import requests
import gzip
import io

# Assuming .base defines AbstractDownloader
from .base import AbstractDownloader
from src.core.exceptions import DownloadError, FileSystemError, NotFoundError, RequestTimeoutError  # Import exceptions

logger = logging.getLogger(__name__)

# Gzip magic number
GZIP_MAGIC_NUMBER = b'\x1f\x8b'


class DocumentDownloader(AbstractDownloader):
    """
    Downloads specific filing documents (e.g., 10-K HTML files) from SEC EDGAR Archives.
    Includes robust handling for potential gzip issues.
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
            cik_no_zeros = str(int(cik))
        except ValueError:
            logger.error(f"Invalid CIK format for URL construction: {cik}")
            return None
        acc_no_dashes = accession_number.replace('-', '')
        url = f"{self.sec_api_settings.edgar_archive_base}/{cik_no_zeros}/{acc_no_dashes}/{filename}"
        return url

    # Overriding download method from base class
    def download(self, cik: str, accession_number: str, filename: str,
                 output_path: Path) -> bool:
        """
        Downloads a specific filing document, checks for gzip magic number
        before attempting decompression, and saves to output_path.
        """
        url = self._build_document_url(cik, accession_number, filename)
        if not url:
            logger.error(
                f"Could not build download URL for {cik}/{accession_number}/{filename}"
            )
            return False

        try:
            output_path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error(
                f"Failed to create directory {output_path.parent}: {e}")
            # Decide if this should raise or just return False
            # Raising might be better to halt if file system is problematic
            raise FileSystemError(
                f"Failed to create directory {output_path.parent}: {e}") from e

        logger.info(
            f"Attempting to download document from {url} to {output_path.name}"
        )

        try:
            # Use base class helper, enable streaming
            response = self._make_request(
                url, headers=self.headers, stream=True,
                timeout=120)  # Increased timeout slightly

            # Get raw content
            try:
                # Consider memory usage for very large files if reading all at once
                raw_content = response.content
            finally:
                response.close()  # Ensure connection is closed

            if not raw_content:
                logger.warning(
                    f"Downloaded empty content for {filename} from {url}")
                return False  # Treat empty content as failure

            # --- Robust Gzip Handling ---
            final_content_to_write = None
            # Check the first two bytes for the gzip magic number
            is_gzipped = raw_content.startswith(GZIP_MAGIC_NUMBER)
            logger.debug(
                f"File {filename}: Starts with gzip magic number? {is_gzipped}"
            )

            if is_gzipped:
                logger.info(
                    f"Attempting gzip decompression for {filename} based on magic number..."
                )
                try:
                    final_content_to_write = gzip.decompress(raw_content)
                    logger.info(
                        f"Gzip decompression successful for {filename}.")
                except gzip.BadGzipFile:
                    logger.error(
                        f"BadGzipFile error for {filename} despite magic number match. Writing raw content as fallback."
                    )
                    final_content_to_write = raw_content  # Fallback to writing raw
                except Exception as gz_err:
                    logger.error(
                        f"Gzip decompression failed for {filename}: {gz_err}. Writing raw content as fallback."
                    )
                    final_content_to_write = raw_content  # Fallback
            else:
                # If no magic number, assume it's not gzipped (or already decompressed by requests/server)
                logger.info(
                    f"No gzip magic number found for {filename}. Assuming raw content is correct."
                )
                final_content_to_write = raw_content
            # --- End Gzip Handling ---

            if not final_content_to_write:
                logger.error(
                    f"Content processing failed, no final content to write for {filename}."
                )
                return False

            # Write the final content (decompressed or raw) to the file
            logger.debug(
                f"Writing final content ({len(final_content_to_write)} bytes) to {output_path.name}..."
            )
            with open(output_path, 'wb') as f_out:
                f_out.write(final_content_to_write)

            logger.info(f"Successfully wrote content to {output_path.name}")
            return True

        # --- Exception Handling ---
        except NotFoundError:
            logger.warning(f"Document not found at {url} (404)")
            return False
        except RequestTimeoutError:
            logger.error(f"Timeout downloading document from {url}")
            return False
        except DownloadError as e:
            logger.error(
                f"Download failed for document {url}: {e}",
                exc_info=False)  # Less verbose logging for common errors
            return False
        except FileSystemError:  # Re-raise FileSystemError from directory creation
            raise
        except IOError as e:
            logger.error(f"Failed to write data to {output_path}: {e}",
                         exc_info=True)
            return False
        except Exception as e:
            logger.error(
                f"Unexpected error during document download for {url}: {e}",
                exc_info=True)
            return False
