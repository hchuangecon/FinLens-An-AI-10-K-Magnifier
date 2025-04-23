# src/extraction/downloaders/incremental.py

import logging
from pathlib import Path
import requests  # For specific exceptions
from datetime import date
import gzip
import io

from .base import AbstractDownloader
from src.core.exceptions import DownloadError, NotFoundError, RequestTimeoutError  # Import custom exceptions

logger = logging.getLogger(__name__)


class IncrementalDownloader(AbstractDownloader):
    """
    Downloads SEC daily master index files (master.YYYYMMDD.idx).
    """

    def build_index_url(self, target_date: date) -> str:
        """Constructs the URL for the daily index file for a given date."""
        year = target_date.year
        quarter = (target_date.month - 1) // 3 + 1
        date_str_url = target_date.strftime('%Y%m%d')
        # Use base_url from sec_api_settings
        url = f"{self.sec_api_settings.base_url}/Archives/edgar/daily-index/{year}/QTR{quarter}/master.{date_str_url}.idx"
        return url

    # Changed download signature to reflect fetching content for a specific date
    def download(self, target_date: date) -> str | None:
        """
        Downloads the daily index file content for the specified date.

        Args:
            target_date: The specific date for which to download the index file.

        Returns:
            The text content of the index file if successfully downloaded,
            otherwise None (e.g., if file not found - 404).

        Raises:
            RequestTimeoutError: If the request times out.
            DownloadError: For other non-404 HTTP errors or network issues.
        """
        url = self.build_index_url(target_date)
        logger.info(
            f"Attempting to download daily index file for {target_date} from {url}"
        )

        try:
            # Use the base class helper. Streaming isn't necessary for small index files.
            # Use default SEC headers. Timeout can be standard.
            response = self._make_request(url,
                                          headers=self.headers,
                                          stream=False,
                                          timeout=60)

            # Check encoding, SEC files are often latin-1 or similar, but try utf-8 first
            try:
                content = response.content.decode('utf-8')
            except UnicodeDecodeError:
                logger.warning(
                    f"UTF-8 decode failed for {url}, trying latin-1.")
                try:
                    content = response.content.decode('latin-1')
                except Exception as decode_err:
                    logger.error(
                        f"Failed to decode content from {url} with UTF-8 or latin-1: {decode_err}",
                        exc_info=True)
                    # Raise specific error? Or return None? Let's raise for now.
                    raise DownloadError(f"Failed to decode content from {url}",
                                        url=url)

            logger.info(
                f"Successfully downloaded index content for {target_date}.")
            return content

        except NotFoundError:
            # It's common for index files not to exist (weekends, holidays), so just log info and return None
            logger.info(
                f"Daily index file not found for {target_date} at {url} (404). This may be normal."
            )
            return None  # Return None, not an error, for 404s on index files
        except RequestTimeoutError as e:
            # Re-raise the specific error from _make_request
            logger.error(
                f"Timeout downloading index for {target_date} from {url}")
            raise e
        except DownloadError as e:
            # Catch other network/HTTP errors from _make_request
            # Treat these as more serious errors than a 404 for index files
            logger.error(
                f"Download failed for index {target_date} at {url}: {e}",
                exc_info=True)
            raise e  # Re-raise the error
        except Exception as e:
            # Catch any other unexpected errors
            logger.error(
                f"An unexpected error occurred during index download for {target_date}: {e}",
                exc_info=True)
            raise DownloadError(
                f"Unexpected error downloading index {target_date}: {e}",
                url=url)

    def build_quarterly_index_url(self, year: int, quarter: int) -> str:
        """Constructs the URL for the quarterly compressed index file."""
        # Use base_url from sec_api_settings
        url = f"{self.sec_api_settings.base_url}/Archives/edgar/full-index/{year}/QTR{quarter}/master.gz"
        return url

    def download_quarterly_index_content(self, year: int,
                                         quarter: int) -> str | None:
        """
        Downloads and decompresses the quarterly index file for the specified year/quarter.

        Args:
            year: The year of the index.
            quarter: The quarter (1-4) of the index.

        Returns:
            The decompressed text content of the index file, or None if not found/error.

        Raises:
            RequestTimeoutError: If the request times out.
            DownloadError: For other non-404 HTTP errors or network issues.
        """
        url = self.build_quarterly_index_url(year, quarter)
        logger.info(
            f"Attempting to download quarterly index for {year}-Q{quarter} from {url}"
        )

        try:
            # Use base helper, streaming IS needed for potential Gzip decompression in memory
            response = self._make_request(url,
                                          headers=self.headers,
                                          stream=True,
                                          timeout=180)  # Longer timeout

            # Decompress gzipped content in memory
            # Use response.content instead of response.raw with gzip.open for simplicity here
            # Ensure response is closed after reading content if not using context manager in _make_request
            try:
                # Read compressed content into memory first
                compressed_content = response.content
                # Decompress using gzip
                decompressed_content = gzip.decompress(compressed_content)
            except gzip.BadGzipFile as e:
                logger.error(f"Failed to decompress gzip file from {url}: {e}")
                raise DownloadError(f"Bad gzip file received from {url}",
                                    url=url)
            except Exception as e:
                logger.error(
                    f"Error handling response content or decompressing {url}: {e}",
                    exc_info=True)
                raise DownloadError(
                    f"Failed to process response stream from {url}", url=url)
            finally:
                response.close(
                )  # Explicitly close response since we read .content

            # Now decode the decompressed bytes
            try:
                content = decompressed_content.decode('utf-8')
            except UnicodeDecodeError:
                logger.warning(
                    f"UTF-8 decode failed for {url} (after gzip), trying latin-1."
                )
                try:
                    content = decompressed_content.decode('latin-1')
                except Exception as decode_err:
                    logger.error(
                        f"Failed to decode decompressed content from {url} with UTF-8 or latin-1: {decode_err}",
                        exc_info=True)
                    raise DownloadError(
                        f"Failed to decode decompressed content from {url}",
                        url=url)

            logger.info(
                f"Successfully downloaded and decompressed index content for {year}-Q{quarter}."
            )
            return content

        except NotFoundError:
            logger.info(
                f"Quarterly index file not found for {year}-Q{quarter} at {url} (404)."
            )
            return None
        except RequestTimeoutError as e:
            logger.error(
                f"Timeout downloading quarterly index for {year}-Q{quarter} from {url}"
            )
            raise e
        except DownloadError as e:
            logger.error(
                f"Download failed for quarterly index {year}-Q{quarter} at {url}: {e}",
                exc_info=True)
            raise e
        except Exception as e:
            logger.error(
                f"An unexpected error occurred during quarterly index download for {year}-Q{quarter}: {e}",
                exc_info=True)
            raise DownloadError(
                f"Unexpected error downloading quarterly index {year}-Q{quarter}: {e}",
                url=url)

    # Note: This class doesn't determine *which* dates to download.
    # The calling service (e.g., pipeline orchestrator) will loop through
    # the desired dates and call this download method for each one.
