# Release lookup fix: round 4

- Theme: detailed control flow and data handling.
- Reviewer: GPT-6 Astra, direct coordinator review.
- Reviewed base: `55b5ed5fb91e75e081429e9da7c3501378c3640d`.

The negated assignment checks curl's exit status before interpreting its HTTP
status. The quoted case statement permits exactly 200 and 404. Only 404 writes
`found=false`, which is the existing publication step's condition.

The endpoint is fixed to the public GitHub API, the tag is validated earlier
in the workflow, and the token comes from the existing secret-backed
environment. Response bodies are discarded; credentials are not printed.

No outstanding issue found. No release or package was created by this review.
