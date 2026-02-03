"""Thin HTTP client for the Streaming Availability API (SAPI).

This module implements the adapter pattern to handle authentication,
request building, and telemetry for SAPI endpoints.
"""

import logging
import time
import requests
from src.config import SAPI_RETRY_POLICY

logger = logging.getLogger(__name__)


class SapiClient:
    """Client for interacting with the Streaming Availability API.

    Attributes:
        session (requests.Session): Persistent session for HTTP requests.
        base_url (str): The root URL for the SAPI service.
    """

    def __init__(self, api_key: str, api_host: str, base_url: str):
        """Initializes the SapiClient with RapidAPI credentials.

        Args:
            api_key: The x-rapidapi-key secret.
            api_host: The x-rapidapi-host hostname.
            base_url: The base URL for the API.
        """

        self.session = requests.Session()
        self.base_url = base_url.rstrip("/")

        self.session.headers.update(
            {"x-rapidapi-key": api_key, "x-rapidapi-host": api_host}
        )

        logger.info("SapiClient initialized with base_url=%s", self.base_url)

    @SAPI_RETRY_POLICY
    def fetch_data(self, endpoint: str, query_params: dict) -> dict:
        """Fetches and parses JSON data from a specific SAPI endpoint.

        This method is wrapped by a retry policy to handle transient
        network and server-side errors automatically.

        Args:
            endpoint: The API path (e.g., '/shows/search/filters').
            query_params: Dictionary of URL parameters for the request.

        Returns:
            dict: The parsed JSON response from the server.

        Raises:
            requests.exceptions.RequestException: If the request fails
                after all retry attempts are exhausted.
        """

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        start_ts = time.perf_counter()

        try:
            logger.debug(
                "SAPI_REQUEST_START endpoint=%s params=%s", endpoint, query_params
            )
            response = self.session.get(url, params=query_params)
            response.raise_for_status()

            duration = (time.perf_counter() - start_ts) * 1000
            logger.info(
                "SAPI_REQUEST_SUCESS endpoint=%s status=%s latency_ms=%.2f",
                endpoint,
                response.status_code,
                duration,
            )

            return response.json()

        except requests.exceptions.RequestException as e:
            duration = (time.perf_counter() - start_ts) * 1000
            logger.error(
                "SAPI_REQUEST_FAILED endpoint=%s latency_ms=%.2f error=%s",
                endpoint,
                duration,
                str(e),
            )

            raise
