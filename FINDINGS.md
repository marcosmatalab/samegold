# Findings

Twenty adversarial rounds, and until now what each one found lived in a commit message. A
commit message is not a document: nobody looking for "how does this repository go wrong" reads
`git log`, and the findings that matter are the ones that recur.

**Ordered by what they teach, not by when they happened.** The first section is the findings
that revealed a CLASS - a shape of mistake that turned out to have other instances, usually
found by going looking for them. The second is the ones that were expensive and specific. The
third is the classes themselves, with every appearance linked.

Every row cites the commit that fixed it. Where a test now prevents it, that test is named; a
finding with no test beside it is a finding that is only prevented by remembering, and this
table says so rather than implying otherwise.

---

## The findings that revealed a class

### `ELSE 'accepted'`: everything the system did not understand became revenue

| | |
|---|---|
| **What** | The Databricks lane classified events with `CASE WHEN <bad> THEN reason ... ELSE 'accepted'`. A predicate that evaluates to NULL matches no `WHEN`, so the row fell to the `ELSE`. Three events the generator emits **in order to be rejected** were booked as `accepted` and contributed **2.767e19 cents** of January revenue - six and a half million times the contract's ceiling for one line - from 428 lines. |
| **How found** | The lane was deployed and run for the first time. `close_month` died with `DELTA_CAST_OVERFLOW_IN_TABLE_WRITE`, and the overflow was the *symptom*; the revenue figure behind it was the defect. |
| **Why invisible** | The same rules were declared twice in the same file, once as expectations and once as a `CASE`, and NULL meant the opposite thing in each: `expect_all_or_drop` treats not-TRUE as not-satisfied and drops the row, a `CASE` falls through. The expectations were right by accident of semantics. Every test compared the two on records built with `bronze_schema()`, where the columns are BIGINT and no predicate is ever NULL - **the right rules on the wrong types**. |
| **Prevented by** | The classification is GENERATED from one `RULES` declaration; acceptance is a positive conjunction (`WHEN <every rule holds> THEN 'accepted'`) rather than the leftover branch; the branch that remains is unreachable and calls `raise_error`. `tests/spark/test_adversarial_records.py::test_a_rule_that_cannot_answer_quarantines_the_row_instead_of_accepting_it` injects a rule that is NULL by construction, because the real rules can no longer be undecidable and the property would otherwise hold vacuously. |
| **Commits** | `326aaef`, `d687813`, `c8c4a07` |

### The width of a literal decided a business rule

| | |
|---|---|
| **What** | `unit_price_cents > 1000000`. The literal is INT32, and Spark coerces the *other operand* to the literal's type - so on a STRING column the value `9223372036854775807` was cast to INT32, overflowed, and non-ANSI Spark returned NULL. |
| **How found** | Looking for the mechanism behind the finding above, and measuring it rather than reasoning about it. |
| **Why invisible** | Nothing in the repository distinguished `1000000` from `1000000L`. A test that reads a literal's VALUE cannot see its TYPE, and `test_every_lane_compares_against_the_contracts_bounds_and_nothing_else` had been reading values, correctly, for rounds. |
| **Prevented by** | Every bound literal in Spark-dialect SQL carries `L`; every bound in the PySpark lane goes through `_bound()`, which casts to bigint. `tests/fast/test_contract_documents.py::test_the_spark_dialect_bound_literals_carry_their_width` and `::test_the_pyspark_lane_builds_its_bounds_with_a_declared_width` read the AST, not the text, because the files quote the bare spelling in the comments that explain why it was wrong. DuckDB is exempt **by measurement**: it raises a binder error rather than answering NULL. |
| **Commits** | `d687813`; the measured table is in `docs/limits.md` |

### A check that supplies its own inputs is not a check

| | |
|---|---|
| **What** | Three separate instances. (1) The conservation invariant took all five of its terms from one query, so the identity was algebraic and could not fail - it passed for the same reason `1 = 1` passes. (2) The pairwise rule-coverage test asked whether ANY record in the matrix broke both rules of a pair, and passed with a whole rule's breakers deleted, because records generated for one pair incidentally covered five others. (3) The parity matrix built its records with `bronze_schema()` and so compared the right rules on types the lane never had. |
| **How found** | (1) an adversarial review. (2) **falsifying my own test** immediately after writing it. (3) the deployment. |
| **Why invisible** | All three passed, repeatedly, and reported the thing they were supposed to check. |
| **Prevented by** | (1) `conservation_against_ledger` compares the generator's by-construction count against the reference's independent recount - five quantities, two derivations. (2) coverage is ATTRIBUTABLE: a pair counts as covered only by a record generated for it and tagged in `boundary`, and both falsifications are re-run. (3) the matrix is evaluated on the declared types AND on STRING columns with ANSI pinned off. |
| **Commits** | `bef164c`, `c8c4a07`, `326aaef` |

### The measurement dirtied the thing it was measuring

| | |
|---|---|
| **What** | Every evidence record this repository has ever published carried `tree_dirty: true`, meaning "the code that ran is in no commit". Four independent causes, each hiding the next: Python's text mode wrote CRLF so a second git on the same checkout saw 37 modified files; a stale `.git/index` that neither git would refresh; **the evidence sweep counted its own output as an uncommitted change**, so the first claim saw a clean tree and every claim after it saw the file the sweep had just written; and the fix for that sliced `line[3:]` on a status string whose first line had already been `.strip()`ed, eating one character of the only path that mattered. |
| **How found** | Trying to produce one record with honest provenance, after the policy said provenance has to name a commit. |
| **Why invisible** | The flag errs *safe*. A false "uncommitted tree" understates the evidence, so nobody chased it - for eighteen rounds. And the fourth cause had a unit test that passed, because the test fed the parser strings written by hand with both spaces present, which is not the shape the caller ever produces. |
| **Prevented by** | `EvidenceStore.append` and the renderer write LF explicitly; `_code_changes` splits on whitespace and excludes `evidence/`; `tests/fast/test_seeds.py` carries the stripped shape taken from a real `git status`; `tests/fast/test_evidence_gate.py` asserts the chain has no CRLF. The provenance column now NAMES the commit, which is what made the caveat checkable at all. |
| **Commits** | `16af667`, `b4151a6`, `5adbbe4`, `0bfcff1` |

### The thing that announces an action and the thing that performs it were two

| | |
|---|---|
| **What** | `scripts/databricks_run.sh run-full-refresh` printed `==> FULL REFRESH: ...` and then ran the pipeline without `--full-refresh-all`. The dispatch was `SAMEGOLD_FULL_REFRESH=1 require_cli; require_auth; step_run`, and a bash assignment prefixed to a command applies to **that command only**: the variable existed for `require_cli` and was gone before `step_run` looked. |
| **How found** | A Free Edition run spent on a refresh that did not happen. The update failed with `DELTA_MERGE_INCOMPATIBLE_DATATYPE: StringType and LongType` on `new_qty` - exactly the schema conflict a full refresh exists to clear - and the output had no banner in it, which nobody checked. |
| **Why invisible** | Reading the `case` block cannot find it. The text is correct; the semantics of the shell are not. Reproduced in four words: `f() { :; }; g() { echo "${FLAG:-EMPTY}"; }; FLAG=1 f; g` prints `EMPTY`. |
| **Prevented by** | The command is built as an array, the banner is derived from that array, and the script REFUSES to run if the two disagree. `tests/fast/test_databricks_catalog_step.py` executes the real script against a stub CLI and asserts on the argv the CLI was invoked with - present for `run-full-refresh`, absent for `run` - and a sweep test rejects the same assignment form anywhere in `scripts/*.sh`. |
| **Commits** | this round |

### A comment predicted the risk precisely and the setting did not prevent it

| | |
|---|---|
| **What** | `development: true` on the pipeline, with a comment stating that a development pipeline does not retry a failed update and that on Free Edition a retry loop is the daily quota. One `bundle run` produced **six failed updates in fourteen minutes** - five automatic retries. |
| **How found** | Reading the run history after the failure above. |
| **Why invisible** | The comment was a prediction about a setting nobody had exercised. The reference ties retry behaviour to how the update was TRIGGERED - the UI's *Run now* "disables pipeline retries", updates through Jobs or the API get "automatic retry and restart behavior" - and this lane is started by a job. The first correction then said no bundle setting controls it, which was **also wrong**: `pipelines.numUpdateRetryAttempts` has a documented default of "Five for triggered pipelines", and five is exactly what was observed. |
| **Prevented by** | `pipelines.numUpdateRetryAttempts: "0"` and `pipelines.maxFlowRetryAttempts: "0"` in the pipeline's `configuration:` block, `max_retries: 0` on every job task, and the measurement in `docs/limits.md`. The overrides have not been exercised against a workspace; the default they override is what was measured, and the document says which is which. |
| **Commits** | this round |

### A constant with the shape of a measurement, in the field that reports the outcome

| | |
|---|---|
| **What** | `publish_evidence.py` reported whether the lane worked with `MAX(details:update_progress.state)`. `MAX` on a string is the ALPHABETICAL maximum, and over the states an update passes through - CREATED, WAITING_FOR_RESOURCES, INITIALIZING, SETTING_UP_TABLES, RUNNING, COMPLETED, FAILED, CANCELED - `W` sorts last. The field was a constant. It published `WAITING_FOR_RESOURCES` for update `44a237b3`, which `databricks pipelines get` reports COMPLETED, and it would have published the same word for the update that FAILED that morning. |
| **How found** | The lane's first correct run. Somebody read the record and compared one field against the workspace. |
| **Why invisible** | The value is a legal member of the set, so it looks like an answer. `MAX` is well defined on a string, and the query returns one row per update - nothing errors, nothing is empty. The anchor `dbx:update.last_state` would have accepted it, and the gate that keeps documents from getting ahead of their run checks that the document matches the RECORD, not that the record means anything. |
| **Prevented by** | `max_by(state, timestamp)`, and the class swept: `test_no_statement_takes_a_max_or_min_of_a_non_ordinal_column` refuses `MAX`/`MIN` over any argument in the lane's SQL that is not a timestamp or plainly numeric. `tests/spark/test_databricks_event_log_query.py` runs the lane's own query - extracted from the source, with only `event_log()` substituted for a view - over a synthetic log carrying the real sequence, and requires COMPLETED for an update that completes and FAILED for one that fails. Falsified both ways: with `MAX` restored, both report `WAITING_FOR_RESOURCES`. |
| **Commits** | `8c9faa7`; confirmed in the workspace by `064a451` |

Two things came out of it that are worth more than the fix. The CTE that chose WHICH update to
describe took the most recent to leave any event - during a retry loop, one that has not
finished - and now takes the most recent to reach a terminal state. And `details:path` on a
STRING column turned out to be Databricks SQL (measured: it raises on pyspark 4.2.0, while
`get_json_object` works on both), so the lane's SQL was changed to the portable accessor -
which is what made the query executable outside a workspace at all, and therefore what made
finding this in a test rather than in a record possible.

### The comparison a file declared as its reason for existing, never executed

| | |
|---|---|
| **What** | `gold_close.py` maintains a Type 2 dimension with AUTO CDC and says, in its own comments, that comparing it against the OSS lane's hand-written `MERGE` is the point of having both. The first run compared them for the first time: **78 versions and 18 closed rows against 75 and 15.** |
| **How found** | The lane ran. Nothing in the repository compared the two dimensions, so there was nowhere else it could have been found. |
| **Why invisible** | Both sides were individually well-formed - sixty customers, sixty open rows, `open_rows = customers` on both - so every property either lane checked about ITSELF held. The difference only exists between them, and PARITY.md had been asserting for rounds that comparing them was the point while nothing did. |
| **Root cause, measured** | The population has 78 distinct `customer_upserted` events and exactly three are heartbeats - an upsert repeating the segment and country the customer already had (`cu-C000028-1`, `cu-C000038-1`, `cu-C000043-1`). 75 + 3 = 78 and 15 + 3 = 18. AUTO CDC's default for SCD Type 2 is a new version whenever ANY column changes, and the source view carries `event_ts` and `event_id`, which change on every upsert by construction - so one version per event was guaranteed. |
| **Which was right** | A contract question, and the contract had already answered it, in `transform.dim_customer_scd2`: "A Type 2 dimension records CHANGES, not heartbeats." Three OSS implementations agree and a round was spent making them agree. This lane was the one that was wrong. |
| **Prevented by** | `track_history_column_list=["segment", "country"]` - a fourth Databricks-only primitive, pinned by the mypy error the open-source signature produces. The next run returned 75 / 60 / 60 / 15. `tests/fast/test_databricks_dimension_parity.py` pins the arithmetic, asserts that no two consecutive versions of a customer are identical, and compares the OSS dimension against the workspace record's shape on every run of the fast lane. Falsified: doctored back to 78/18 it fails and prints both shapes. |
| **Commits** | `8c9faa7`; confirmed in the workspace by `064a451` |

The row-level half of that comparison is still a skip. Four matching aggregates do not prove the
same sixty customers have the same seventy-five intervals, and half of a cross-runtime
comparison has to be captured from the runtime.

---

## The expensive specifics

| finding | why it was invisible | prevented by | commit |
|---|---|---|---|
| The Delta lane had been running in CI, **red**, for two days, while `docs/limits.md` described it as "written and not yet executed here". | "Not executed **here**" was doing the work, and a reader has no reason to read "here" as "anywhere the author can see". | `make preflight` runs the whole of CI, not the fast lane; the claim renderer labels a local run as such. | `faaab88` |
| Two engines broke a deduplication tie with **different hash functions** (`sha2` here, `md5` there), so on 48% of colliding pairs they chose different rows. | Both were "a total order". Only the same total order makes the parity claim mean anything, and the generator never emits a colliding pair. | `PAYLOAD_COLUMNS` is shared and `tests/spark` asserts both engines pick the same copy. | `13a7e71` |
| Three order lines at the maximum legal price ended the close outright: Spark refused with `ARITHMETIC_OVERFLOW`, DuckDB published a gross that does not fit its own column. | Every value was a legal BIGINT and no rule bounded either factor. It was the last record shape with no door. | `MAX_LINE_QUANTITY` / `MAX_UNIT_PRICE_CENTS` in the contract, `amount_out_of_range` in the closed enum, and boundary fixtures sitting exactly on and one past each bound. | `8f52142`, `7ec0cca` |
| The bounds were first set nine orders of magnitude too high, and the comment defending them did the arithmetic wrong: it claimed a hundred billion maximum-value lines before the SUM overflows. The true figure was **ninety-two**. | A comment nothing executes. | `test_the_bounds_leave_the_headroom_the_contract_states` recomputes the division. | `7ec0cca` |
| `return_exceeds_sold_qty` sat in the closed enum, **unreachable**, for the whole life of the repository: the generator drew return quantities with `randrange(1, sold + 1)`. | The test asserted the literal string appeared in `transform.py`. | `test_every_quarantine_reason_is_actually_produced_by_a_run` generates a dataset and reads the ledger. | `253dba9` |
| An adversarial reviewer appended two records claiming 999/999 by hand, pointed one at a CI run that does not exist, regenerated the documents, and ran the suite. All 152 tests passed. | The evidence store was a sink. | Hash chain, seeds derived from the commit, provenance shape - ADR 0007, and SG-06 verifies the whole chain. | `813a82b` |
| `databricks bundle validate` answered `Validation OK!` and the deploy died on the first POST: the pipeline resource carried no `name`. | Validate checks syntax, includes and variable resolution. It does not check that the request body it will send is one the API accepts. | Required fields for every resource type asserted from the REST reference in `tests/fast/test_databricks_bundle.py`. | `1521b5f` |
| The catalog step reported `CANCELED` as "the catalog is not there". It was there: the cancel ended the client's wait, not the DDL the warehouse had already admitted. | A false claim about the world, inside its own error message. | Every non-success path re-checks before concluding; `tests/fast/test_databricks_catalog_step.py` drives the step against a stub API. | `25fea06` |
| An `order_line_amended` to a quantity of zero or less was rejected by no lane. | The rule was gated on the two event types that carry `qty`; an amendment carries `new_qty`. All three implementations agreed, so no parity test could see it, and `max(1, ...)` in the generator guaranteed no seed would produce it. | Boundary case 14 emits the zero; the rule covers `new_qty`. | `253dba9` |
| A fast-lane test imported pyspark through `bronze_schema()`, and the `fast` workflow installs `.[dev]`. | Both development machines have the Spark extras, so both agreed with the mistake. | `tests/fast/conftest.py` fails the session, after everything, if Spark was ever imported. | `845bc7a` |
| That same hook then failed the fast lane on **both** development machines for eighteen rounds, because the evidence recorder read its version fingerprint with `__import__("pyspark")`. | pytest printed `57 passed` and nobody read the exit status. CI stayed green: it has no Spark to import. | The fingerprint is read from installed distribution metadata; `test_recording_the_environment_does_not_import_what_it_reports_on`. | `d687813` |
| The two lanes declared the same seven rules **in different orders**, so a record breaking two of them left by different doors. | Every record in the parity matrix broke exactly one rule, so the order decided nothing and twenty records reported agreement. | Pairwise coverage: for every pair of rules, a record generated for that pair must be rejected by both. | `c8c4a07` |

---

## The classes, and where each one has appeared

These are ADR 0006's entries. The ADR argues them; this is the index.

| class | appearances |
|---|---|
| **The conditions are part of the result.** A differential test proves agreement only under the conditions it ran in. | the parity matrix on typed columns (`326aaef`); "not executed here" (`faaab88`); the per-rule test fed hand-written strings (`5adbbe4`) |
| **If acceptance is the default case, everything the system does not understand becomes revenue.** | `ELSE 'accepted'` (`326aaef`); and one round later, `accepted` still being the `ELSE` rather than a stated conjunction (`c8c4a07`) |
| **A rule correct by an argument is not a rule.** State the invariant; do not leave it as the residue of the cases. | the positive conjunction (`c8c4a07`) |
| **Fixing the consequence is not fixing the cause.** | NULL made to fail closed while the predicate stayed NULL (`d687813`) |
| **A fault that erases itself needs a counter, not just a door.** | a value too wide for its column, rescued into a NULL (`d687813`) |
| **A gate whose result is not what its summary line says.** | a green tick reporting the linter (`1521b5f`); the fast lane exiting 1 while printing `57 passed` (`d687813`) |
| **"It works on my machine" and "it works in the repository" are different claims.** | the red Delta job (`faaab88`); pyspark in the fast lane (`845bc7a`); the CRLF that only one git could see (`16af667`) |
| **A message that announces an action is a second implementation of it, and two implementations that are never compared will differ.** | the full-refresh banner that governed nothing (`e002f29`); `development: true` predicting a risk it did not prevent (`e002f29`) |
| **An aggregate that is well defined and answers the wrong question.** `MAX` on a state string is the alphabetical maximum, and it looks like an answer. | the field reporting whether the lane worked (`8c9faa7`) |
| **A comparison a file declares as its reason for existing, and nobody runs.** | the two Type 2 dimensions, 78 against 75 on the first run that compared them (`8c9faa7`) |
| **A closed enum with a member no run can produce is a branch nobody maintains.** | `return_exceeds_sold_qty` (`253dba9`); and the reason an "undecidable" member was refused (`d687813`) |
| **A bound is the size of the fixture that tests it.** | bounds nine orders too high, moving a published figure by scaffolding (`7ec0cca`) |

---

## What this file is not

It is not the evidence. The numbers this repository publishes come from `evidence/history.jsonl`
and are rendered into the documents from the head of that chain - see
`docs/adr/0010-the-chain-is-append-only-and-the-documents-quote-its-head.md`. This file is the
qualitative half: what went wrong, how it was found, and what now stands in the way. Nothing in
it is generated, so nothing in it is checked by a test, and it will drift the way prose drifts.
The commits are the anchor: each one carries the full account, and `git show <hash>` is longer
and more exact than any row above.
