# Release lookup fix: round 3

- Theme: errors and boundaries.
- Reviewer: GPT-6 Astra, direct coordinator review.
- Reviewed base: `55b5ed5fb91e75e081429e9da7c3501378c3640d`.

HTTP 301, 401, 403, 429, 500 and 503 cannot reach note generation or creation.
Empty and malformed status output fail too. Transport errors remain failures
even if curl reports 200 or 404 before failing, covering truncated transfers
and timeouts after headers.

Connection and total request time are bounded. Redirects are not followed,
TLS verification remains enabled, and curl's external configuration is disabled.
Error paths do not write a success-shaped workflow output.

All corresponding executable workflow cases passed. No outstanding issue
found in this single-coordinator pass.
