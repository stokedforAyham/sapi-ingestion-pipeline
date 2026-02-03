"""Configuration and policy definitions for SAPI communication.

This module defines which HTTP errors are considered transient and
configures the global retry policy using the Tenacity library.
"""

import requests
import logging

from tenacity import retry, stop_after_attempt, wait_incrementing, retry_if_exception

logger = logging.getLogger(__name__)

RETRIABLE_STATUS_CODES = {429, 500, 502, 503, 504}


def log_retry(retry_state):
    """Logs details of a failed request before attempting a retry.

    Args:
        retry_state: The current state of the tenacity retry call.
    """

    logger.warning(
        "SAPI_RETRY_DELAY | Attempt: %s | Reason: %s | Waiting: %ss",
        retry_state.attempt_number,
        retry_state.outcome.exception(),
        retry_state.next_action.sleep,
    )


def is_transient_error(exception):
    """Determines if an exception should trigger a retry attempt.

    Retries on connection issues and specific HTTP status codes (429, 5xx).

    Args:
        exception: The exception raised during the HTTP request.

    Returns:
        bool: True if the error is transient and should be retried, False otherwise.
    """

    # 1. Handle Connection issues (always retry)
    if isinstance(exception, requests.exceptions.ConnectionError):
        return True

    # 2. Handle HTTPErrors (only retry 429 and 5xx)
    if isinstance(exception, requests.exceptions.HTTPError):
        status = exception.response.status_code
        return status == 429 or status >= 500

    return False


SAPI_RETRY_POLICY = retry(
    retry=retry_if_exception(is_transient_error),
    wait=wait_incrementing(start=1, increment=1, max=5),
    stop=stop_after_attempt(3),
    reraise=True,
    before_sleep=log_retry,  # This is the hook that handles the monitoring
)
