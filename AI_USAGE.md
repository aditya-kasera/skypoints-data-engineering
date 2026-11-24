# AI usage and engineering ownership

Maintain this log incrementally as AI-assisted work is performed. Separate generated suggestions from decisions reviewed by a person, and record only verification supported by actual evidence.

## Current foundation work

- **AI tool and role:** OpenAI Codex assisted with repository inspection, foundation planning, implementation, and review.
- **AI-assisted tasks and approach:** Inspected the starter repository and environment; followed the user-approved plan to add a minimal `src` package, dependency/test configuration, CI, documentation, and a smoke test. No assessment logic or data transformations were generated.
- **Architectural decisions:** `src` package layout, `pyproject.toml`, Python 3.10 CI, one in-memory DuckDB/pandas smoke test, and CSV/XLSX-only source tracking were specified by the user and approved in the plan. Implementation choices remain subject to user review.
- **Important prompt:** Implement the consolidated foundation plan while preserving source data, excluding the assessment PDF and Windows metadata, and not committing or pushing.
- **AI-assisted review findings and corrections:** Initial inspection found `.gitignore` contained shell heredoc text rather than ignore patterns; it was replaced with repository ignore rules.
- **Verification performed:** `python -m pip install -e '.[test]'` succeeded in the local Python 3.10.15 virtual environment. The first attempt failed because sandbox network access could not reach the package index; it succeeded after network access approval. `python -m pytest` passed: 1 test passed. The assessment PDF and Windows metadata were confirmed ignored with `git check-ignore`. GitHub Actions has not been run. No other verification is claimed.
