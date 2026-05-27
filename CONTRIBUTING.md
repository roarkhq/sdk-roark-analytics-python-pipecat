# Contributing

Thanks for your interest in `pipecat-roark`. Contributions of all sizes — bug reports, docs, tests, and code — are welcome.

## Reporting bugs and requesting features

Use the issue templates: [Bug report](https://github.com/roarkhq/sdk-roark-analytics-python-pipecat/issues/new?template=bug_report.yml) or [Feature request](https://github.com/roarkhq/sdk-roark-analytics-python-pipecat/issues/new?template=feature_request.yml). Include the package version (`pip show pipecat-roark`), Python version, and `pipecat-ai` version when filing a bug.

For security issues, follow [SECURITY.md](./SECURITY.md) instead — do not open a public issue.

## Development setup

This project uses [`uv`](https://github.com/astral-sh/uv) for environment and dependency management.

```bash
uv sync --all-extras
uv run pytest
uv run ruff check .
uv run ruff format --check .
```

Python 3.10+ is required. The package is tested against `pipecat-ai >= 0.0.40, < 1`.

## Pull requests

1. Fork the repo and create a topic branch from `main`.
2. Make focused, single-purpose changes — smaller PRs land faster.
3. Add or update tests for behavior changes.
4. Run `uv run pytest` and `uv run ruff check .` locally before pushing.
5. Open a PR against `main` and fill in the template.
6. CI must be green. A maintainer will review.

We squash-merge all PRs, so commit hygiene inside the branch is less important than a clear PR title and description.

## Code style

- Ruff handles linting and formatting — no manual style debates.
- Public API additions should include a docstring and an example in the README if user-facing.
- Keep the observer surface narrow; prefer extending `RoarkObserverConfig` over adding new entry points.

## Releases

Releases are cut by maintainers via the `Release` workflow on a pushed tag. Contributors do not need to bump versions in PRs.

## License

By contributing, you agree that your contributions will be licensed under the [MIT License](./LICENSE).
