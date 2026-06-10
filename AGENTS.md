I found 31 unique `no mocking` prompts in recent Codex history, across 21 session transcripts. Only one transcript literally started with `no mocking`; most had it later as testing guidance. Consolidated advice:

- Tests must hit real user-facing code paths: CLI commands, REST/API calls, browser UI, real hooks, real ArchiveBox data dirs, real pytest fixtures, and real subprocess/binary behavior.
- No mocking, faking, simulating, monkey patching, handwritten fake objects, fake buses, fake hooks, fake binaries, fake handlers, or direct-post shortcuts when the user path is through UI/extension/CLI.
- No skipped, xfailed, flaky, or “works around platform” tests. Flakiness is treated as a bug, especially on macOS/browser flows.
- Prefer live integration tests over narrow unit tests when behavior depends on browsers, binaries, ArchiveBox crawls, plugins, LLMs, or server state.
- Assertions must validate real correctness: returned values, exit codes, DB rows, filesystem contents, field values, uploaded files, rendered output, and side effects. “No error occurred” or “attribute exists” is not enough.
- Start fixes with failing red tests that reproduce the missing behavior or regression, then implement the minimal fix and confirm the test passes.
- Use realistic setup patterns “like a user would”: events + bus + handlers, real browser pages/CDP sessions, real URLs or `pytest-httpserver`, real rows, real snapshots, real installs, real local browser/server state.
- For ArchiveBox/API tests, use existing `conftest.py` fixtures and test harnesses, real test DB rows/data dirs, and user-facing commands/APIs rather than bespoke helpers.
- For browser/extension tests, trigger behavior through the real extension UI or actual browser session, not direct posting or mocked browser/session objects.
- For binary/provider tests, use real binaries and real installs; verify constraints and final installed package metadata, not just install success.
- For coverage quality, keep tests strict, deterministic, grouped consistently, and use a few larger realistic tests when that gives better surface coverage than many tiny fake unit tests.
- Avoid weakening test coverage, adding compatibility/shim/fallback layers, or guessing from code shape. Trace root causes, verify assumptions with tests/scripts, and let real type/parse errors surface normally.
