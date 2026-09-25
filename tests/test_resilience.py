"""
Tests for resilience against ENTSO-E failures.

Covered:

- the HTTP session used by the ENTSO-E client retries transient failures (connection
  errors, read timeouts, 429 and 5xx responses) with exponential backoff, and the client
  is configured with a timeout;
- outages treat ENTSO-E's "no matching data" as "no outages announced";
- the API key is masked in logs and error messages.

No live API and no database are touched: ENTSO-E is stood in for by a fake client or a
local HTTP server. ``flexmeasures`` must be importable for these (the package __init__
pulls it in); if it is not installed, the whole module is skipped.
"""

import logging
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pandas as pd
import pytest
import requests
from flask import Flask

pytest.importorskip("flexmeasures")

import entsoe  # noqa: E402
from entsoe.exceptions import NoMatchingDataError  # noqa: E402

from flexmeasures_entsoe.prices import regressors_import  # noqa: E402
from flexmeasures_entsoe.utils import (  # noqa: E402
    RETRY_STATUS_CODES,
    create_entsoe_client,
    create_http_session,
    mask_api_key,
)

TZ = "Europe/Amsterdam"
FROM_TIME = pd.Timestamp("2025-06-02 00:00", tz=TZ)
UNTIL_TIME = pd.Timestamp("2025-06-03 00:00", tz=TZ)


class FakeLog:
    def __init__(self):
        self.warnings = []

    def info(self, *args):
        pass

    def debug(self, *args):
        pass

    def warning(self, msg, *args):
        self.warnings.append(msg)


# ---------------------------------------------------------------------------
# HTTP session: exponential backoff on transient failures
# ---------------------------------------------------------------------------


def test_http_session_retry_config():
    """The session retries transient failures, with exponentially growing waits."""
    session = create_http_session(max_retries=4, backoff_factor=3)
    retry = session.get_adapter("https://web-api.tp.entsoe.eu/api").max_retries

    assert retry.total == 4
    assert retry.backoff_factor == 3
    assert set(retry.status_forcelist) == set(RETRY_STATUS_CODES)
    assert {429, 500, 502, 503, 504} <= set(RETRY_STATUS_CODES)
    assert retry.respect_retry_after_header
    # The last error response is handed to entsoe-py, which parses ENTSO-E's error text.
    assert retry.raise_on_status is False
    # Backoff grows exponentially (urllib3 2 retries the first time right away)
    waits = []
    for _ in range(4):
        retry = retry.increment(method="GET", url="/api")
        waits.append(retry.get_backoff_time())
    assert waits == [0, 6, 12, 24]


class FlakyHandler(BaseHTTPRequestHandler):
    """Serves the next status code from ``statuses`` on each GET."""

    statuses: list = []
    calls = 0

    def do_GET(self):
        type(self).calls += 1
        status = type(self).statuses.pop(0)
        body = b"ok" if status == 200 else b"busy"
        self.send_response(status)
        if status == 429:
            self.send_header("Retry-After", "0")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


@pytest.fixture
def flaky_server():
    server = ThreadingHTTPServer(("127.0.0.1", 0), FlakyHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    FlakyHandler.calls = 0
    yield f"http://127.0.0.1:{server.server_address[1]}/api"
    server.shutdown()
    server.server_close()


def test_http_session_retries_transient_errors_until_success(flaky_server):
    """429 and 5xx responses are retried until ENTSO-E answers."""
    FlakyHandler.statuses = [503, 429, 502, 200]
    session = create_http_session(max_retries=5, backoff_factor=0)

    response = session.get(flaky_server, timeout=5)

    assert response.status_code == 200
    assert FlakyHandler.calls == 4


def test_http_session_returns_last_error_response_when_retries_exhausted(
    flaky_server,
):
    """Once retries run out, the last error response is returned, not raised."""
    FlakyHandler.statuses = [503, 503, 503]
    session = create_http_session(max_retries=2, backoff_factor=0)

    response = session.get(flaky_server, timeout=5)

    # Not raised by the session itself, so entsoe-py's raise_for_status() can parse it.
    assert response.status_code == 503
    assert FlakyHandler.calls == 3
    with pytest.raises(requests.HTTPError):
        response.raise_for_status()


def test_http_session_does_not_retry_client_errors(flaky_server):
    """ENTSO-E answers "no matching data" with a 400; retrying that is pointless."""
    FlakyHandler.statuses = [400]
    session = create_http_session(max_retries=5, backoff_factor=0)

    response = session.get(flaky_server, timeout=5)

    assert response.status_code == 400
    assert FlakyHandler.calls == 1


def _app(**config):
    app = Flask(__name__)
    app.config.update(ENTSOE_AUTH_TOKEN="token", **config)
    return app


def test_create_entsoe_client_uses_configured_retries_and_timeout():
    """The ENTSOE_MAX_RETRIES, ENTSOE_BACKOFF_FACTOR and ENTSOE_TIMEOUT settings are used."""
    app = _app(
        ENTSOE_MAX_RETRIES=7,
        ENTSOE_BACKOFF_FACTOR=1.5,
        ENTSOE_TIMEOUT=45,
    )
    with app.app_context():
        client = create_entsoe_client()

    retry = client.session.get_adapter("https://web-api.tp.entsoe.eu/api").max_retries
    assert retry.total == 7
    assert retry.backoff_factor == 1.5
    assert client.timeout == 45
    # entsoe-py's own fixed-delay retry is switched off, so retries don't multiply.
    assert client.retry_count == 1
    assert client.retry_delay == 0


def test_create_entsoe_client_has_defaults():
    """Without settings, requests are still retried and time out."""
    with _app().app_context():
        client = create_entsoe_client()

    retry = client.session.get_adapter("https://web-api.tp.entsoe.eu/api").max_retries
    assert retry.total > 0
    assert retry.backoff_factor > 0
    assert client.timeout is not None


# ---------------------------------------------------------------------------
# Outages: "no matching data" means no outages were announced
# ---------------------------------------------------------------------------


class FakeOutagesClient:
    """Fake ENTSO-E client for outages.

    mode: 'ok' | 'no-data' | 'raise'
    """

    def __init__(self, mode="ok"):
        self.mode = mode

    def query_unavailability_of_generation_units(self, country_code, start, end):
        if self.mode == "no-data":
            raise NoMatchingDataError()
        if self.mode == "raise":
            raise requests.ConnectionError("connection reset")
        return pd.DataFrame(
            {
                "nominal_power": [1000.0],
                "avail_qty": [0.0],
                "start": [FROM_TIME],
                "end": [UNTIL_TIME],
            },
            index=pd.Index([FROM_TIME - pd.Timedelta(days=2)], name="created_doc_time"),
        )


def _collect_outages(client):
    results = regressors_import._collect_country_regressors(
        client=client,
        log=FakeLog(),
        country_code="NL",
        from_time=FROM_TIME,
        until_time=UNTIL_TIME,
        include_load_forecast=False,
        include_wind_solar_forecast=False,
        include_residual_load=False,
        include_outages=True,
        strict=True,
    )
    return {spec[0]: series for spec, _override, series, _is_entsoe in results}


def test_outages_without_matching_data_mean_no_outages():
    """ENTSO-E answers "no matching data" if no outages were announced: 0 MW."""
    saved = _collect_outages(FakeOutagesClient(mode="no-data"))
    assert saved["Generation outages"].to_numpy() == pytest.approx([0.0] * 24)


def test_outages_are_counted():
    """Announced outages still count as unavailable capacity."""
    saved = _collect_outages(FakeOutagesClient())
    assert saved["Generation outages"].to_numpy() == pytest.approx([1000.0] * 24)


def test_outages_query_failure_still_aborts():
    """Other failures to get outages still abort the import."""
    with pytest.raises(requests.ConnectionError):
        _collect_outages(FakeOutagesClient(mode="raise"))


# ---------------------------------------------------------------------------
# The API key (a URL parameter) must not leak into logs or error messages
# ---------------------------------------------------------------------------

SECRET = "my-secret-api-key"


def test_mask_api_key():
    """The key is masked both in URLs and in entsoe-py's logged request parameters."""
    assert mask_api_key(f"/api?a=1&securityToken={SECRET}&b=2") == (
        "/api?a=1&securityToken=***&b=2"
    )
    assert SECRET not in mask_api_key(f"params {{'securityToken': '{SECRET}'}}")


def test_api_key_masked_in_http_errors_and_logs(flaky_server, monkeypatch, caplog):
    """A failing query neither logs the key nor puts it in the raised error."""
    FlakyHandler.statuses = [503, 503, 503]
    monkeypatch.setattr(entsoe.entsoe, "URL", flaky_server)
    client = entsoe.EntsoePandasClient(
        api_key=SECRET,
        session=create_http_session(max_retries=2, backoff_factor=0),
        retry_count=1,
        timeout=5,
    )

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(requests.HTTPError) as error:
            client.query_day_ahead_prices("NL", start=FROM_TIME, end=UNTIL_TIME)

    assert "503" in str(error.value)
    assert SECRET not in str(error.value)
    assert "securityToken" in caplog.text  # the URL was logged, but masked
    assert SECRET not in caplog.text


def test_api_key_masked_in_connection_errors_and_logs(caplog):
    """Neither urllib3's retry warnings nor the raised error show the key."""
    with socket.socket() as s:  # a port nobody listens on
        s.bind(("127.0.0.1", 0))
        closed_port = s.getsockname()[1]
    session = create_http_session(max_retries=2, backoff_factor=0)

    with caplog.at_level(logging.DEBUG):
        with pytest.raises(requests.ConnectionError) as error:
            session.get(
                f"http://127.0.0.1:{closed_port}/api",
                params={"securityToken": SECRET},
                timeout=5,
            )

    assert "Retrying" in caplog.text
    assert SECRET not in caplog.text
    assert SECRET not in str(error.value)
    assert error.value.__cause__ is None and error.value.__suppress_context__
