# ChatGPT Prompt History — Spark Skew Lab

## Record note
This document is a reconstructed summary of the project-related ChatGPT prompts and tasks, prepared after implementation. It is not a verbatim export of every original chat message. The prompts below should be treated as representative only; they must be checked against the actual conversation before being described as exact historical prompts.

## AI tool used
- ChatGPT

## 1. Understand the challenge
Representative prompt:
"Help me break down the Spark Skew Lab challenge requirements into implementation tasks, required deliverables, validation rules, tests, and documentation."

Purpose:
To understand the scope of the assignment and organize the work.

## 2. Plan the pipeline
Representative prompt:
"Help me design a PySpark pipeline for handling skewed data, including baseline processing, salting, validation, reconciliation, and measurable performance comparisons."

Purpose:
To reason about the pipeline structure and the trade-offs between baseline and salted processing.

## 3. Data validation and error handling
Representative prompt:
"Review the input-validation approach for missing or malformed fields, unexpected schema fields, negative values, and unknown references. Suggest clear failure or quarantine behavior."

Purpose:
To examine invalid-input handling and make validation behavior testable.

## 4. Duplicate event handling
Representative prompt:
"Help me define and test deterministic behavior when duplicate event IDs appear in the input, including how the later duplicate should be handled."

Purpose:
To make duplicate-event behavior explicit and repeatable.

## 5. Empty input and salt validation
Representative prompt:
"Suggest tests for empty input and invalid salting configuration, including a requirement that salting uses multiple buckets."

Purpose:
To cover edge cases beyond the normal successful batch.

## 6. Idempotent batch processing
Representative prompt:
"Help me test batch idempotency: when the same batch ID and same input are submitted again after success, the previous successful result should be reused rather than creating another metrics record."

Purpose:
To verify repeat-run behavior for the same successful batch.

## 7. Automated tests
Representative prompt:
"Review the pipeline tests and identify missing coverage for normal input, empty input, malformed data, duplicate events, invalid references, and salting behavior."

Purpose:
To check the breadth of automated validation.

## 8. End-to-end verification
Representative prompt:
"Help me verify an end-to-end run with a seeded input dataset, checking accepted and rejected counts, reconciliation, group counts, salts, and repeat-run idempotency."

Purpose:
To collect reproducible evidence that the pipeline runs and its output counts reconcile.

## 9. Benchmark methodology
Representative prompt:
"Help me compare baseline and salted processing with a fixed random seed, controlled configuration, warm-up, repeated measurements, and median runtime. Explain why a single benchmark result should not be treated as a universal performance guarantee."

Purpose:
To make the performance comparison more reproducible and interpret results cautiously.

## 10. README and project handoff
Representative prompt:
"Help me organize the README so another reviewer can understand the architecture, setup, execution commands, validation steps, benchmark method, results, limitations, and design trade-offs."

Purpose:
To improve reproducibility and make the project easier to review.

## 11. Final review
Representative prompt:
"Review the project deliverables against the challenge checklist. Separate verified results from assumptions and list any remaining gaps that should be disclosed."

Purpose:
To identify incomplete documentation or validation without overstating what has been verified.

## Limitations of this record
- This is a reconstructed topic-based record, not an exact export of the original prompts.
- The representative wording must not be claimed as verbatim historical text.
- Only ChatGPT is listed because that is the AI tool reported by the candidate.
- Implementation decisions, test outcomes, and benchmark figures should be verified against the repository and actual run logs before final submission.
