import pytest
import requests
import responses
from src.client.client import SapiClient
from src.config import SAPI_RETRY_POLICY

# --- FIXTURES ---
# Logic: Avoids repeating setup code in every test.
@pytest.fixture
def client():
    return SapiClient(
        api_key="test_key",
        api_host="test_host",
        base_url="https://api.test.com"
    )

# --- 1. POSITIVE TESTING (The Contract) ---
@responses.activate
def test_fetch_data_success(client):
    # Logic: Prove the client parses a standard successful JSON response.
    mock_json = {"items": [{"id": 1}], "next_cursor": "abc"}
    responses.add(
        responses.GET,
        "https://api.test.com/endpoint", 
        json=mock_json,
        status=200
    )

    result = client.fetch_data("endpoint", {"param": "val"})

    assert result == mock_json
    assert responses.calls[0].request.headers["x-rapidapi-key"] == "test_key"

# --- 2. NEGATIVE TESTING (The Fragility) ---
@responses.activate
def test_fetch_data_unauthorized(client):
    # Logic: Prove that 401 errors (which shouldn't be retried) fail immediately.
    responses.add(
        responses.GET,
        "https://api.test.com/endpoint", 
        status=401
    )

    with pytest.raises(requests.exceptions.HTTPError):
        client.fetch_data("endpoint", {})

    # Assert it only tried once (no retry logic triggered for 401)
    assert len(responses.calls) == 1

# --- 3. CONSTRAINTS (The Limits) ---
@responses.activate
def test_fetch_data_empty_response(client):
    # Logic: Prove the client handles valid but empty/null payloads without crashing.
    responses.add(
        responses.GET,
        "https://api.test.com/endpoint", 
        json={},
        status=200
    )

    result = client.fetch_data("endpoint", {})
    assert result == {}

# --- 4. THE BRANCHES (The Implicit Loop) ---
@responses.activate
def test_fetch_data_retry_until_success(client):
    # Logic: Force the execution path through the decorator's retry loop.
    url = "https://api.test.com/endpoint"

    # Branch Path: Failure -> Failure -> Success
    responses.add(responses.GET, url, status=429)
    responses.add(responses.GET, url, status=429)
    responses.add(responses.GET, url, json={"status": "ok"}, status=200)

    result = client.fetch_data("endpoint", {})

    assert result == {"status": "ok"}
    assert len(responses.calls) == 3

@responses.activate
def test_fetch_data_max_retries_exhausted(client):
    # Logic: Prove the "Stop" branch of the decorator works.
    url = "https://api.test.com/endpoint"

    # Simulate infinite failures
    responses.add(responses.GET, url, status=500)

    with pytest.raises(requests.exceptions.HTTPError):
        client.fetch_data("endpoint", {})

    # If your config says stop_after_attempt(3), this should be 3
    assert len(responses.calls) == 3
