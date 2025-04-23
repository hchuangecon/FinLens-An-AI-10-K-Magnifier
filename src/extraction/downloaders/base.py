# finlens/extraction/downloaders/base.py

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from src.core.rate_limiting import RateLimiter
from src.config.settings import AppSettings

logger = logging.getLogger(__name__)


class AbstractDownloader(ABC):
    """
    Abstract Base Class for all data downloaders from SEC sources.
    """

    def __init__(self, settings: AppSettings, rate_limiter: RateLimiter):
        """
        Initializes the downloader with necessary configurations and rate limiter.

        Args:
            settings: The application settings object.
            rate_limiter: The shared rate limiter instance.
        """
        self.settings = settings
        self.rate_limiter = rate_limiter
        self.sec_api_settings = settings.sec_api  # Convenience alias
        self.headers = {  # Standard headers for SEC web requests
            "User-Agent": self.sec_api_settings.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "www.sec.gov"  # Default host, might be overridden
        }
        self.api_headers = {  # Headers for data.sec.gov API
            "User-Agent": self.sec_api_settings.user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Host": "data.sec.gov"
        }
        logger.info(f"{self.__class__.__name__} initialized.")

    @abstractmethod
    def download(self, *args, **kwargs) -> bool:
        """
        Primary method to perform the download operation specific to the subclass.

        Actual signature will vary based on the downloader type.
        Should return True on success, False on failure.
        """
        pass

    # We could add more common utility methods here if needed,
    # for example, a shared helper for making HTTP requests
    # that incorporates rate limiting and basic error handling.

    def _make_request(self,
                      url: str,
                      headers: dict | None = None,
                      stream: bool = False,
                      timeout: int = 60):
        """
        Internal helper method to make rate-limited HTTP GET requests.
        Handles basic error checking and raises appropriate custom exceptions.

        Args:
            url: The URL to request.
            headers: Specific headers for this request (defaults to self.headers).
            stream: Whether to stream the response content.
            timeout: Request timeout in seconds.

        Returns:
            requests.Response object on success.

        Raises:
            RequestTimeoutError: If the request times out.
            NotFoundError: If the resource returns a 404 status.
            DownloadError: For other HTTP errors or request issues.
        """
        from src.core.exceptions import DownloadError, NotFoundError, RequestTimeoutError  # Local import
        import requests  # Local import

        if headers is None:
            headers = self.headers  # Use default web headers if none provided

        self.rate_limiter.wait()  # Apply rate limiting BEFORE the request
        logger.debug(f"Making request to: {url}")
        try:
            response = requests.get(url,
                                    headers=headers,
                                    stream=stream,
                                    timeout=timeout)
            response.raise_for_status()  # Raises HTTPError for 4xx/5xx
            return response
        except requests.exceptions.Timeout:
            logger.error(f"Timeout requesting {url}")
            raise RequestTimeoutError(
                f"Request timed out after {timeout} seconds", url=url)
        except requests.exceptions.HTTPError as e:
            status_code = e.response.status_code
            logger.warning(f"HTTP error {status_code} for {url}")
            if status_code == 404:
                raise NotFoundError(url=url)
            else:
                raise DownloadError(f"HTTP error {status_code}",
                                    url=url,
                                    status_code=status_code)
        except requests.exceptions.RequestException as e:
            logger.error(f"Request exception for {url}: {e}")
            raise DownloadError(f"Network request failed: {e}", url=url)
        except Exception as e:  # Catch any other unexpected error during request
            logger.error(f"Unexpected error during request to {url}: {e}",
                         exc_info=True)
            raise DownloadError(f"Unexpected request error: {e}", url=url)
