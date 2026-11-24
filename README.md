# SkyPoints Data Engineering Technical Assessment

This repository is the foundation for the SkyPoints data engineering assessment. It contains the supplied source files and a minimal Python package. Business transformations and Snowflake SQL deliverables will be added after their requirements are established.

## Requirements

- Python 3.10 or newer
- `pip`

## Setup

Create and activate a virtual environment, then install the package and test dependencies:

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
python -m pip install -e ".[test]"
```

## Execution and testing

No assessment pipeline or business logic is implemented yet. Run the foundation smoke test with:

```bash
python -m pytest
```

GitHub Actions runs the same test command on pushes and pull requests using Python 3.10.

## Repository layout

```text
data/raw/                         Supplied CSV and Excel source files
sql/                              Reserved for Snowflake SQL deliverables
src/skypoints_data_engineering/   Installable Python package
tests/                            Automated tests
```

The assessment PDF and operating-system metadata are kept out of Git. Raw CSV and Excel inputs are preserved as supplied.
