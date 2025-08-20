import os
import requests
import pytest


def test_websearch_backend():
    """Test the websearch backend via its HTTP interface."""
    url = os.environ.get("WEBSEARCH_FUNCTION_URL")
    key = os.environ.get("WEBSEARCH_FUNCTION_KEY")
    if not url or not key:
        pytest.skip("WEBSEARCH_FUNCTION_URL and WEBSEARCH_FUNCTION_KEY must be set")

    headers = {"x-functions-key": key, "Content-Type": "application/json"}
    payload = {"query": "Microsoft Azure"}

    response = requests.post(url, headers=headers, json=payload, timeout=15)
    response.raise_for_status()

    data = response.json()
    assert data, "Empty JSON response"
