from starlette.testclient import TestClient


def test_gzip_response_when_accepted(testclient: TestClient):
    """Responses larger than the minimum size should be gzipped when Accept-Encoding includes gzip."""
    response = testclient.get("/openapi.json", headers={"Accept-Encoding": "gzip"})
    assert response.status_code == 200
    assert response.headers.get("content-encoding") == "gzip"
    # TestClient auto-decompresses; verify the body is still valid JSON
    assert "openapi" in response.json()


def test_no_gzip_response_when_not_accepted(testclient: TestClient):
    """Responses should not be gzipped when Accept-Encoding does not include gzip."""
    response = testclient.get("/openapi.json", headers={"Accept-Encoding": "identity"})
    assert response.status_code == 200
    assert response.headers.get("content-encoding") is None


def test_gzip_response_has_vary_header(testclient: TestClient):
    """Gzipped responses should include a Vary: Accept-Encoding header."""
    response = testclient.get("/openapi.json", headers={"Accept-Encoding": "gzip"})
    assert response.status_code == 200
    assert "Accept-Encoding" in response.headers.get("vary", "")
