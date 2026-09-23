# AGENTS.md

Guidance for AI coding agents working in this repository. This file is read by tools that follow the `AGENTS.md` convention (Claude Code via `CLAUDE.md`, OpenAI Codex, and others). If you use a tool that reads a different file (Cursor: `.cursor/rules`, GitHub Copilot: `.github/copilot-instructions.md`, Aider: `CONVENTIONS.md`), point it at this file or mirror the relevant parts.

**This is a public open-source repository.** Treat every commit, comment, PR description, and code suggestion as world-readable. The Open-source guardrails section below is the most important part of this file — read it before making any change.

---

## Project overview

`pipecat-roark` is a [Pipecat](https://github.com/pipecat-ai/pipecat) observer that ships call lifecycle, transcripts, tool calls, and stereo recordings to the [Roark](https://roark.ai) analytics platform. It is provider-agnostic and runtime-agnostic — the same code runs self-hosted and on Pipecat Cloud.

- Python 3.10+
- Async-native (built on `httpx` + Pipecat's asyncio runtime)
- Single public surface: `RoarkObserver` and `RoarkObserverConfig`
- Tested against `pipecat-ai >= 0.0.104, < 1`

## Project structure

```
.
├── src/pipecat_roark/         # The library — keep the public surface narrow
│   ├── __init__.py            # Re-exports
│   ├── observer.py            # RoarkObserver (the main entry point)
│   ├── client.py              # Roark webhook + upload HTTP client
│   ├── _types.py              # Private type aliases (underscore = internal)
│   └── py.typed               # PEP 561 marker — ship type hints to users
├── tests/                     # Pytest, colocated test files
├── examples/                  # Runnable demos (not packaged)
├── pyproject.toml             # Hatchling build, ruff + pytest config
├── uv.lock                    # Lockfile — commit changes alongside deps
└── .github/workflows/         # CI, Release, Claude review
```

## Commands

Use [`uv`](https://github.com/astral-sh/uv) for everything.

```bash
uv sync --all-extras            # Install runtime + dev + examples deps
uv run pytest                   # Run tests
uv run pytest tests/test_observer.py::test_x   # Run a single test
uv run ruff check .             # Lint
uv run ruff format .            # Format
uv run ruff format --check .    # Check formatting (CI mode)
uv run mypy src                 # Type-check the library
```

**Always run `uv run ruff check .` and `uv run pytest` before pushing.** CI will reject otherwise.

## Code conventions

- **Public vs private**: anything under a module starting with `_` (e.g. `_types.py`) is internal and may change without notice. The public surface is whatever is re-exported from `src/pipecat_roark/__init__.py` — be deliberate about what lands there. A helper that only `RoarkObserver` uses is internal: give it a `_`-prefixed module and keep it out of `__all__`.
- **Prefer extending the existing public class over adding a new one.** Every public class is a wiring step in someone's pipeline. A feature that can be a keyword argument on `RoarkObserver` should be one.
- **Type hints required** on all public functions and methods. `py.typed` ships with the package, so users rely on these.
- **`Any` is not an escape hatch.** Annotate the real type. When a type is needed only for annotations, import it under `if TYPE_CHECKING:`.
- **Import at module scope.** `pipecat` is a hard dependency, so import its types at the top of the module rather than inside a function that runs per frame. Reserve function-local imports for genuinely optional dependencies (e.g. `opentelemetry`), and say so in the module docstring.
- **No defensive fallbacks for what the API guarantees.** `getattr(frame, "field", default)` on a declared dataclass field hides typos rather than surviving anything. If you are unsure a field exists across the supported range, check it (see *Supported Pipecat versions*) instead of guessing.
- **Async everywhere**: this is an async library. New I/O must be async. Do not introduce blocking calls inside the observer hot path.
- **Imports**: ruff handles sorting (`select = ["I"]` is on). Don't fight it.
- **Line length**: 100 (set in `pyproject.toml`).
- **Ruff rules in play**: `E, F, I, B, UP, N`. If you disagree with a rule, fix the code — don't add `# noqa`.
- **Comments**: write them only when the *why* is non-obvious (a Pipecat API quirk, a workaround for a known bug, a non-obvious invariant). Don't narrate the code. A comment restating what the next line does, or explaining that something does *not* matter, should be deleted.
- **Docstrings carry the *why*.** A method's non-obvious invariant belongs in its docstring, where it reaches users through `help()` and the published API docs, not in a comment above one line of its body.
- **Logging**: use the existing logger pattern in the module. Never log API keys, request bodies that contain transcripts, or PII.

## Testing

- Tests live in `tests/` and use pytest with `asyncio_mode = "auto"` (no `@pytest.mark.asyncio` needed).
- Add a test for any behavior change. Public surface changes without a corresponding test are not acceptable.
- **Never make real network calls in tests.** Mock `httpx.AsyncClient` (or the `RoarkClient`) — tests must run offline and deterministically.
- **Mock the I/O boundary, not the library.** Outside of network calls, prefer real objects: real Pipecat frames, a real `TracerProvider` with an in-memory exporter. Asserting against a mock of the thing under test proves only that the mock was called.
- **A test that cannot fail is not a test.** If an assertion would hold with the behavior removed (two clock reads that could return the same value, an `assert x <= cap` that was never near the cap), tighten it or drive the input so it can actually break.
- **Never commit a real API key**, recording URL, or transcript fixture that contains real user content. Use synthetic fixtures.

## Pull requests & commits

- Branch off `main`. Keep PRs focused on one logical change.
- PRs are **squash-merged**, so the PR title becomes the commit message on `main`. Make it conventional and descriptive: `fix: stop dropping interrupted assistant turns`, `feat: support custom audio sample rates`, `docs: clarify Pipecat Cloud setup`.
- Fill in the PR template. Tick the "no secrets / no customer data" checkbox honestly.
- CI must be green. Don't merge with red checks.
- Update `CHANGELOG.md` under `[Unreleased]` for any user-visible change (added / changed / fixed / removed).

## Supported Pipecat versions

- The package declares `pipecat-ai>=0.0.104,<2`, which spans both majors. The version in `.venv` is one point in that range, not the contract.
- Any change touching a Pipecat API must be verified at **both ends** of the range before it ships. CI has jobs for this; run it locally first:
  `uv run --isolated --with "pipecat-ai==0.0.108" --with "pytest>=8" --with "pytest-asyncio>=0.23" --with "opentelemetry-sdk>=1.24" --with "httpx>=0.27" pytest -q`
- Before relying on a Pipecat attribute, class hierarchy or module path, check it on the oldest and newest supported versions rather than the one that happens to be installed. Record which versions you checked in the commit message.

## Releases

- Publishing is automatic: pushing to `main` runs the `Release` workflow, which compares the `version` in `pyproject.toml` against existing tags. A version with no matching tag is tested across the Python matrix, published to PyPI via Trusted Publishing, then tagged and released on GitHub by the workflow itself.
- So the version bump **is** the release trigger. A PR that should ship a release bumps `version` in `pyproject.toml` and adds the matching `CHANGELOG.md` section; a PR that should not, leaves both alone and files its entry under `[Unreleased]`.
- Never push a `vX.Y.Z` tag by hand — the workflow creates it, and an existing tag is what tells it there is nothing to publish.

---

## Open-source guardrails

This repository is public on GitHub and published to PyPI as `pipecat-roark`. Anyone — including competitors, customers, prospective customers, security researchers, and random internet users — can read everything you write here. The rules below exist so that "fine in a private repo" mistakes don't become "indexed by Google forever" mistakes.

### Never include in code, comments, commits, PRs, or issue replies

- **Internal infrastructure details.** No references to internal AWS accounts, account IDs, stage names, internal hostnames, Temporal workflows, GraphQL schemas, database tables, internal microservices, or any other component of the Roark backend that customers don't see.
- **Internal tooling.** No links to Linear, Notion, Slack, internal dashboards, internal Grafana boards, internal runbooks, Google Docs, Figma files, or any other internal system. If a piece of context lives behind a Roark SSO login, it doesn't belong in this repo.
- **Internal identifiers.** No internal ticket IDs (e.g. `PROJ-1234` from a private tracker), internal PR numbers from other repos, internal Slack thread links, employee handles, or codenames for unreleased features.
- **Customer information.** Never name a Roark customer, paste their data, reference their use case, or mention contract terms. If a customer's bug report drove a fix, describe the bug behavior — not the customer.
- **Unreleased product details.** Don't mention features that aren't shipped on roark.ai or in public docs. No internal roadmap, no "we're planning to add X", no references to private design discussions.
- **Credentials and secrets.** No API keys, OAuth tokens, presigned URLs, webhook secrets, or `.env` values — even fake-looking ones. Use `rk_live_replace_me` style placeholders only.
- **Personally identifiable information.** No real names, emails, phone numbers, or recordings in fixtures, examples, or test data. Synthetic only.
- **Other private Roark repos.** Don't reference internal package names, internal repo names, or anything else that 404s for an external reader. If you're unsure whether a repo is public, check whether it appears at <https://github.com/orgs/roarkhq/repositories?type=public>.

### PR descriptions and commit messages

Write them so an external contributor (or a future maintainer with no Roark context) can fully understand them.

- **Self-contained.** No "see internal ticket PROJ-1234", no "as discussed in #eng-channel", no "per the design doc". State the user-facing problem and how this PR addresses it.
- **Describe behavior, not internal reasoning.** "Fix turn boundaries collapsing inter-turn silence" — yes. "Fix the bug Acme Corp reported in last Tuesday's call" — no.
- **No internal jargon.** Roark has internal terms for things; use the public name (the one in the README and docs.roark.ai). If a concept doesn't have a public name yet, describe it generically.
- **Reviewer mentions.** Tag GitHub usernames only. Don't paste Slack handles or internal team aliases.
- **Reference issues by GitHub number** in *this* repo (`#42`). Never reference issues in private repos.

### Code comments

- Treat every comment as part of the public API documentation — it ships in the source distribution on PyPI.
- Don't reference internal incidents, internal team decisions, or "the way we do it on the platform side". If a comment needs internal context to make sense, rewrite it so it stands alone.
- No TODOs with internal owners (`# TODO(daniel): ...`). Either fix it, file a GitHub issue, or write the TODO generically.

### Examples and fixtures

- Use generic personas (`Acme`, `the user`, `the agent`). Don't use real company names — including non-customers.
- Recording URLs in examples should point to public domains (`https://example.com/sample.mp3`) or be obvious placeholders.
- `examples/` is shipped expectation: copy-pasteable code that works against a fresh Roark API key. If it depends on internal-only behavior, it doesn't belong here.

### README and docs

- Link to `docs.roark.ai` for product concepts, not internal docs.
- Keep the dependency-version messaging conservative — pin ranges, document the tested version.
- Don't promise SLAs, uptime numbers, or roadmap dates.

### When in doubt

If you're not sure whether something is OK to commit, **don't**. Ask in the PR description ("@maintainer is this OK to expose?") or open a draft PR and tag a maintainer for review before pushing further. The cost of pausing is low; the cost of indexing internal info on a public repo is high (Git history is forever, even after a force-push, because of forks and caches).

---

## Where to find more

- [README.md](./README.md) — user-facing docs and quick start
- [CONTRIBUTING.md](./CONTRIBUTING.md) — contribution flow and dev setup
- [SECURITY.md](./SECURITY.md) — how to report vulnerabilities (privately)
- [CHANGELOG.md](./CHANGELOG.md) — release history
- [docs.roark.ai/integrations/pipecat](https://docs.roark.ai/integrations/pipecat) — product documentation
