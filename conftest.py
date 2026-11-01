"""Local external-service endpoints for tests and executable documentation."""

import os
import re

import pytest
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Response


@pytest.fixture(scope="session")
def archivedotorg_server():
    with HTTPServer(host="127.0.0.1") as server:
        server.expect_request(re.compile(r"^/save/https?://")).respond_with_handler(
            lambda request: Response(
                "Local CI capture confirmed",
                headers={"Content-Location": "/web/20261031000000/" + request.environ["RAW_URI"][len("/save/") :]},
            ),
        )
        yield server


@pytest.fixture(autouse=True)
def local_archivedotorg_endpoint(archivedotorg_server):
    previous = os.environ.get("ARCHIVEDOTORG_SAVE_URL")
    os.environ["ARCHIVEDOTORG_SAVE_URL"] = archivedotorg_server.url_for("/save/")
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop("ARCHIVEDOTORG_SAVE_URL", None)
        else:
            os.environ["ARCHIVEDOTORG_SAVE_URL"] = previous
