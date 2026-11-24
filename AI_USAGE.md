# AI usage and engineering ownership

Maintain this log incrementally as AI-assisted work is performed. Separate generated suggestions from decisions reviewed by a person, and record only verification supported by actual evidence.

## Current foundation work

- **AI tool and role:** OpenAI Codex assisted with repository inspection, foundation planning, implementation, and review.
- **AI-assisted tasks and approach:** Inspected the starter repository and environment; followed the user-approved plan to add a minimal `src` package, dependency/test configuration, CI, documentation, and a smoke test. No assessment logic or data transformations were generated.
- **Architectural decisions:** `src` package layout, `pyproject.toml`, Python 3.10 CI, one in-memory DuckDB/pandas smoke test, and CSV/XLSX-only source tracking were specified by the user and approved in the plan. Implementation choices remain subject to user review.
- **Important prompt:** Implement the consolidated foundation plan while preserving source data, excluding the assessment PDF and Windows metadata, and not committing or pushing.
- **AI-assisted review findings and corrections:** Initial inspection found `.gitignore` contained shell heredoc text rather than ignore patterns; it was replaced with repository ignore rules.
- **Verification performed:** `python -m pip install -e '.[test]'` succeeded in the local Python 3.10.15 virtual environment. The first attempt failed because sandbox network access could not reach the package index; it succeeded after network access approval. `python -m pytest` passed: 1 test passed. The assessment PDF and Windows metadata were confirmed ignored with `git check-ignore`. GitHub Actions has not been run. No other verification is claimed.

## Milestone 1 source profiling

- **AI tool and role:** OpenAI Codex used repository inspection and read-only Python/pandas/openpyxl commands to profile the supplied sources and draft a canonical-model proposal.
- **AI-assisted task and approach:** Read `AGENTS.md`, inspected the actual `AUS.xlsx`, `IND.csv`, and `USA.csv` files, checked headers, sheet names, row counts, source cell/data types, missing values, date encodings, IDs, names, and categories. Wrote `docs/source-profiling.md` from observed values and explicitly separated facts, assumptions, unresolved questions, and recommendations. Source data were not modified.
- **Important prompt/approach:** Profile source files before business logic; do not fabricate global member IDs or update timestamps; distinguish observed facts from assumptions and unresolved business questions.
- **Architectural decisions and review:** The source-aware schema is a recommendation only. No canonical model or business rule has been approved by a human in this work.
- **Review findings:** AUS has a literal `NULL` DOB and invalid enrollment text `2021-13-13`; USA stores compact numeric dates, including two ambiguous 7-digit values; IDs 1–3 repeat across feeds; `Mike` appears in AUS and USA without sufficient evidence to merge. The original assessment PDF is absent from this checkout and reachable Git history, so specification differences could not be verified.
- **Verification performed:** Read-only inspection confirmed one visible AUS worksheet (`Sheet1`) and 3 rows per source. Python profiling printed schemas, source representations, and missing-value counts; AUS Python datetime cells and its malformed date string were checked explicitly, and all possible month/day/year splits were enumerated for the compact USA values. No tests were run for this documentation-only task. No original source file was edited.

## Assessment architecture documentation

- **AI tool and role:** OpenAI Codex inspected the repository, all three active source files, and the complete local three-page Incubyte assessment PDF, then drafted the requirements transcription, architecture, and traceability documentation.
- **AI-assisted work:** Created `docs/assessment-requirements.md`, `docs/architecture.md`, and `docs/requirements-traceability.md`; corrected the PDF comparison, date/key-scope assumptions, and open questions in `docs/source-profiling.md`. No business logic, code, dependency, repository-configuration, or source-data changes were made.
- **Human-reviewed decisions:** The user approved the architecture in principle and clarified that the active inventory is exactly AUS.xlsx, IND.csv, and USA.csv; Member Name checks must be scoped and must not cause cross-country merges/quarantine; the Feb. 29 convention is an implementation assumption; and sample country routing uses an explicit demo-only filename map. The proposed filename convention and date/time handling are documented as proposals/assumptions, not source contracts.
- **Important approach:** Follow the PDF as authoritative, separate it from user-provided project constraints, compare it to actual source layouts, preserve unresolved facts, and avoid inventing member identity or business update timestamps.
- **Review finding and correction:** The prior source-profile text said the PDF was unavailable. The PDF is now present locally at `reference/assessment.pdf` and ignored by Git; the source profile and requirements transcription were corrected to reflect it and keep it out of the public repository.
- **Verification performed:** `python -m pytest` passed (1 test). `git diff --check` passed. A read-only local Markdown-link check found all local targets present. A search found no stale references to removed duplicate input variants. GitHub Actions and Snowflake SQL execution were not run; no such verification is claimed.
