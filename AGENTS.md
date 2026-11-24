# Engineering instructions

- Read the requirements and inspect relevant existing code before changing files. Follow the agreed architecture, keep changes small, and avoid unnecessary abstractions or dependencies.
- Never invent business rules, source fields, identifiers, timestamps, or mappings. Record assumptions and unresolved requirements explicitly.
- Preserve raw source data. Do not silently discard invalid records; add meaningful validation and error handling where behavior is implemented.
- Write or update tests with implementation changes. Run relevant tests and report actual outcomes; never claim success without execution evidence.
- Explain significant architectural choices and trade-offs. Prefer correctness, clarity, reproducibility, and maintainability over unnecessary complexity.
- Review the Git diff before any commit. Do not commit, push, deploy, or perform destructive actions without explicit approval.
