# ADR 0009 - the privacy controls run in code, because the platform cannot enforce them

**Status** accepted, 2026-09-01

## Context

The exam guide asks for anonymisation "including hashing, tokenization, suppression and
generalization", for row filters and column masks, for pipelines that detect and mask PII, and
for purging under a retention policy.

Databricks Free Edition can demonstrate almost none of it. There are no account groups, so
`is_account_group_member` is false for everyone and a row filter is a policy nobody is subject
to. Writing the SQL and screenshotting the absence of an error would be a demonstration of
nothing.

## Decision

Implement the controls in code, where they execute and can be tested, and declare the platform
equivalents in SQL for a workspace that has groups. Say which is which.

Two design choices inside that:

- **The exposure check runs over the OUTPUT, not at the point of masking.** Masking is applied
  by someone who remembers to apply it; a check over gold rows catches a new column that
  carries an identifier under a different name, which is the failure that actually happens.
- **The purge deletes AND vacuums.** On a lakehouse a `DELETE` does not delete: the rows stay
  in the previous version and time travel returns them until the files are gone. A purge that
  stops at the `DELETE` does not meet a retention policy, and the test fails if it does.

## What we gave up

Enforcement. A control in the pipeline can be bypassed by anyone who writes a different
pipeline; a platform control cannot. The project says this rather than implying otherwise, and
the Databricks lane carries the declarations for a workspace that can enforce them.
