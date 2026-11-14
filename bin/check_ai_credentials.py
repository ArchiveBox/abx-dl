#!/usr/bin/env -S uv run python
"""Keep paid CI extraction conditional on usable Anthropic credentials."""

import json
import os
import urllib.error
import urllib.request
from pathlib import Path


def unavailable_reason(status: int, response: dict) -> str:
    error = response.get("error", {})
    if response.get("type") != "error" or not isinstance(error, dict):
        return ""
    if status == 401 and error.get("type") == "authentication_error":
        return "Anthropic rejected the API credentials"
    if (
        status == 400
        and error.get("type") == "invalid_request_error"
        and str(error.get("message", "")).startswith("Your credit balance is too low to access the Anthropic API.")
    ):
        return "Anthropic API credits are unavailable"
    return ""


def main() -> None:
    key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    reason = "ANTHROPIC_API_KEY is not configured"
    if key:
        request = urllib.request.Request(
            "https://api.anthropic.com/v1/messages",
            data=json.dumps(
                {
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 1,
                    "messages": [{"role": "user", "content": "Reply OK."}],
                },
            ).encode(),
            headers={"content-type": "application/json", "anthropic-version": "2023-06-01", "x-api-key": key},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                body = json.load(response)
            if body.get("type") != "message" or not body.get("content"):
                raise SystemExit("Anthropic availability request returned no message")
            reason = ""
        except urllib.error.HTTPError as error:
            body = json.loads(error.read())
            reason = unavailable_reason(error.code, body)
            if not reason:
                raise SystemExit(f"Anthropic availability request failed: HTTP {error.code}: {body}") from error
    if reason:
        with Path(os.environ["GITHUB_ENV"]).open("a") as output:
            for plugin in ("AUTOTAG", "AUTOSUMMARY", "CLAUDECHROME"):
                output.write(f"{plugin}_ENABLED=False\n")
            output.write(f"CI_ANTHROPIC_UNAVAILABLE={reason}\n")
        print(json.dumps({"status": "noresults", "reason": reason}))
    else:
        print(json.dumps({"status": "available"}))


if __name__ == "__main__":
    main()
