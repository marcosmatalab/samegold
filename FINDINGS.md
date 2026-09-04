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
| **Commits** | `e002f29` |

### A comment predicted the risk precisely and the setting did not prevent it

| | |
|---|---|
| **What** | `development: true` on the pipeline, with a comment stating that a development pipeline does not retry a failed update and that on Free Edition a retry loop is the daily quota. One `bundle run` produced **six failed updates in fourteen minutes** - five automatic retries. |
| **How found** | Reading the run history after the failure above. |
| **Why invisible** | The comment was a prediction about a setting nobody had exercised. The reference ties retry behaviour to how the update was TRIGGERED - the UI's *Run now* "disables pipeline retries", updates through Jobs or the API get "automatic retry and restart behavior" - and this lane is started by a job. The first correction then said no bundle setting controls it, which was **also wrong**: `pipelines.numUpdateRetryAttempts` has a documented default of "Five for triggered pipelines", and five is exactly what was observed. |
| **Prevented by** | `pipelines.numUpdateRetryAttempts: "0"` and `pipelines.maxFlowRetryAttempts: "0"` in the pipeline's `configuration:` block, `max_retries: 0` on every job task, and the measurement in `docs/limits.md`. The overrides have not been exercised against a workspace; the default they override is what was measured, and the document says which is which. |
| **Commits** | `e002f29` |

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

The capture arrived and the row-level comparison runs: **the two dimensions agree on all
seventy-five rows**, as multisets and as per-customer histories. Nothing appeared that the
aggregates could not see. What did appear is the next finding.

### A comparison that normalised away the difference it existed to catch

| | |
|---|---|
| **What** | The row-by-row comparison reduced every timestamp with `str(value)[:19]` and asked `row not in theirs`. Doctored against the real capture, it **passed** with every instant moved an hour (`+01:00`) and **passed** with 77 rows where 75 were expected, two of them repeats. |
| **How found** | By falsifying it before trusting it. It went green the moment the capture landed, and a comparison that passes the first time it ever runs has told you nothing about itself. |
| **Why invisible** | The truncation was written to make the two sides comparable at all - a workspace emits `2026-01-01T00:00:00+00:00`, the generator `2026-01-01T00:00:00.000000Z` - and it worked. Cutting nineteen characters is exactly the operation that makes unequal things equal, and it was introduced as plumbing rather than as a decision. The set question had the same shape: `not in` reads as "is missing", and the file's whole finding was about rows being EXTRA. |
| **Class** | The same class as the `MAX` over a state string in the round before: an operation that is well defined, does what it says, and answers a different question than the one being asked. Both survived because their output looked right. |
| **Prevented by** | Instants are parsed, and a timestamp without a zone is refused by name rather than assumed to be UTC. Versions are compared as a `Counter`, and the row count is asserted separately. A third test compares each customer's history as an ORDERED sequence, so the same versions on different customers is a failure. Five doctorings of the committed capture - zone, duplicates, a shifted boundary, a naive timestamp, swapped customers - each fail two or three of them. |
| **Commits** | `9a3c0c0` |

### Evidence that could not name the run that produced it

| | |
|---|---|
| **What** | The row-level dimension capture was exported by hand and committed as a bare JSON array. Nothing in it, or anywhere else, tied it to the run whose tables it came from - so a later run replacing `SG-DBX-01.json` would have left the row-by-row comparison passing GREEN against rows the workspace no longer held. |
| **How found** | Written down as a risk in this repository's own "asked and not done" list at the end of the round that created it, and read back by the person who had asked for the capture. It had not gone wrong yet; there had only been one run. |
| **Why invisible** | It is the failure mode of a green test. Nothing fails, nothing changes, and the dataset quietly stops describing the system - which is the same shape as the parity matrix that ran on the wrong column types and the Delta job that was red for two days while a document called it "not executed here". |
| **Class** | A check is only as current as the data it reads, and a comparison against a captured half is a comparison against a snapshot. Provenance is what makes a snapshot expire loudly instead of silently. |
| **Prevented by** | The capture is written by `publish_evidence.py`, in the same task and the same session as the record, from the same table - so its `provenance.update_id` and the record's are the same read of the same event log. The commit travels with the deploy as a bundle variable rather than being written afterwards by the fetching machine, because a value the fetcher supplies agrees with itself by construction and can be re-stamped onto stale rows. Two ties are checked: the update ids must match, and the record's six dimension aggregates must recompute from the captured rows - the second holds with no header at all, which is what covers the one capture whose header was typed rather than measured. Six doctorings fail it, including a record advanced to a later update. |
| **Commits** | `02adf05` |

### The evidence rested on a population nobody could regenerate

| | |
|---|---|
| **What** | The Databricks lane's second close restated January from 14 198 046 cents to 25 582 615, because 573 events for January arrived after it closed. Those 573 events were produced by a script in `/tmp` on one machine. Nothing in this repository could make them again, so every figure that close published rested on data no reader could reproduce. |
| **How found** | By asking where the population came from, before touching the four failing tests. Nothing was red because of it: the run was correct, the record was correct, and the gap was in what a reader could do with either. |
| **Why invisible** | The evidence machinery checks that a figure comes from a record and that a record names a commit. It has nothing to say about whether the DATA behind the record can be produced twice, and this repository's whole argument is that it can - seeds derive from the commit sha precisely so that a stranger can rerun them. The one population that mattered most had opted out of that. |
| **Class** | The premise inverted. Same family as "not executed here" and the mutation score nobody could recompute: a claim whose support exists only on the machine that made it. |
| **Prevented by** | `samegold generate-late --seed 20260901 --late-seed 20260904`, in `src/samegold/generator/late.py`: generate the base population, generate a second, keep the events whose id the base did not have, write them under `batch=late-<stamp>` so they cannot collide in the landing volume. `tests/fast/test_late_arrivals.py` pins what it produces - **573 events in 269 batch directories**, by type {order_placed 420, order_line_amended 63, customer_upserted 21, return_registered 69} and by event month {2026-01 553, 2026-02 16, 2026-03 4} - and that two runs produce the same bytes. The OSS lane over the reproduced population computes the restated close **to the cent**: 25 582 615 gross, 23 268 535 net, 793 lines, 126 returns, 32 rejected. |
| **Commits** | this round |

### A guard that protected the run and not the population

| | |
|---|---|
| **What** | The second close made `tests/fast/test_databricks_dimension_parity.py` report that AUTO CDC and the hand-written MERGE had produced different dimensions, **92 versions against 75**. They had not. The workspace had ingested 1328 events and the OSS half of the comparison was computed over 755. |
| **How found** | The failure was read before it was fixed. Taking it at its word would have meant "fixing" a parity difference that did not exist - by relaxing an assertion, or by writing 92 in as the expected number, which is the same thing with more typing. |
| **Why invisible** | The file HAD a guard for exactly this shape, added one round earlier: the capture's `update_id` against the record's. It passed. Both files did come from update `289286cc`. Sameness of run is not sameness of population, and nothing was asking the second question. A check that is well defined, does what it says, and answers a different question than the one being asked - the fourth time this repository has found that, after `MAX` over a state string, `str(value)[:19]`, and `row not in theirs`. |
| **Prevented by** | The population is now CHOSEN BY THE RECORD: each documented population is generated and counted, and the one whose count matches `rows.bronze_events` is what the OSS half is computed over. A record matching neither fails by name. `test_both_halves_of_the_comparison_describe_the_same_population` asks the question the update id could not, three ways - events against the record's count, capture rows against the record's own row count, OSS versions against capture rows. Verified by simulating the second run's record against the first run's capture: the failures name the population, not the parity. |
| **Commits** | this round |

### The workspace ran what was deployed, and nobody had deployed

| | |
|---|---|
| **What** | The second run's record carried no `deploy` key and `publish_evidence.py` wrote no `dim_customer_scd2.json`, although both had been in the repository for a day. `databricks bundle run` runs what was DEPLOYED, and no `bundle deploy` had happened since the commits that added them. |
| **How found** | By the absence of a field. The task ended SUCCESS, the pipeline was green, the close was correct, and there was no error anywhere - one fault with two symptoms, neither of which is an error message. |
| **Why invisible** | Nothing compares the deployed code with the tree. A run is green about the code it ran, and the code it ran is invisible from the repository. This is the "it works on my machine" class with the machines swapped: the repository was right and the workspace was old. |
| **What made it detectable** | The record carries its own deploy provenance, and the ABSENCE of that key was the evidence. A record that named a commit would have named the wrong one; a record with no `deploy` block at all could only have been written by a notebook that predated the block. That is an argument for the round-25 decision to carry the commit into the workspace through the deploy rather than write it afterwards from the fetching machine, and it is worth stating because the argument was theoretical when the decision was made. |
| **Prevented by** | `require_fresh_deployment` in `scripts/databricks_run.sh`. It reads the `deploy_commit` parameter off the DEPLOYED job with one `databricks jobs list --name ... --expand-tasks` and REFUSES to run when it is not `HEAD`; `SAMEGOLD_RUN_STALE=1` is the way to say you meant it. Seven answers are distinguished rather than two - a sha, "unknown", no such job, no such parameter, two tasks disagreeing, an unreadable answer, no interpreter - because collapsing any of them makes the caller guess. Eleven tests in `tests/fast/test_databricks_catalog_step.py` drive the real script against the stub CLI. |
| **Note** | This was left open on the grounds that it needed a workspace to develop against. It did not: `step_deploy` already carried the commit into the job's `base_parameters`, and `test_databricks_catalog_step.py` already ran the whole script against a fake `databricks` on PATH. "Needs a workspace" was a reason nobody had tried, which is the same shape as the round-12 lanes that were red for two days behind "not executed here". |
| **Commits** | `748d40a` |

### The guard was named for the population and measured the row count

| | |
|---|---|
| **What** | `test_both_halves_of_the_comparison_describe_the_same_population` selected which population to generate by matching `rows.bronze_events` - a COUNT - and that was the whole of the tie between the two halves of the dimension comparison. Two count-preserving mutations of the generator, both measured: **reordering the `countries` list literal** leaves 1328 rows, 96 upserts, 4 heartbeats, 92 versions, 60 customers, 60 open and 32 closed rows - every published number identical - and gives **thirty customers a different history**; **renaming the skus** changes 1216 values and **all nineteen** parity tests still pass. |
| **How found** | Writing the fix for the finding above. The message on that guard's third assertion said "That IS a parity difference", which it could not know, and asking what else it could not know produced the mutation. |
| **Why invisible** | Every existing tie is over a NUMBER. The count guard compares cardinality; close parity compares money, and customer attributes are in no money column while a sku cancels out of `qty * unit_price_cents`; the dimension comparison reads only the dimension. The first mutation therefore surfaced as "the hand-written MERGE and AUTO CDC produced different dimensions" - a true-sounding sentence about two runtimes, sending its reader to look for a divergence that did not exist - and the second surfaced as nothing at all. **The distance between a check's name and its measurement is the whole finding**: a guard called "both halves describe the same population" that asks how many rows there are. |
| **Prevented by** | The workspace publishes a `population` section - digest, `digest_rows`, `rows_outside_the_digest`, `columns` - over every bronze row it ingested, and `samegold.generator.late.population_digest` recomputes it. `tests/spark/test_databricks_population_digest.py` EXECUTES the notebook's own statement, extracted rather than restated, against the Python one over the real population; falsified with one country and one sku, and the schema ties falsified by permuting the projection and the clamp set. The comparison is the FIRST assertion of the guard and names what it cannot separate: the generator moving here, or the volume re-seeded there. |
| **What defining it found** | Two things reading it would not have. The three "corrupt" lines are **truncated objects**, so the two JSON readers need not agree about them - Python sees nothing, local Spark nulls the row, and whether a partial record keeps its leading fields is a setting - so the domain asks for an `event_id` AND an `arrival_ts`, which no truncated line has under either behaviour, and what falls outside is COUNTED: `digest_rows + rows_outside_the_digest = rows.bronze_events`. And the generator emits `9223372036854775808` for two events, one past the top of a BIGINT, so the table holds NULL where Python holds an integer: the digests differed on two rows out of 1325 until the renderer applied the declared range. That one is not inferred - `bad_events` in the committed record reports `unit_price_cents: null` for exactly those two ids. |
| **Commits** | `57e2a13` |

### The fix went out from a tree that did not contain the document describing it

| | |
|---|---|
| **What** | `require_fresh_deployment` closed "the workspace ran what was deployed, and nobody had deployed". The FINDINGS.md entry recording that it was closed was written and **not committed**. So the first deploy after the fix went out from a tree carrying that uncommitted document, and the record it produced said `deploy.tree_dirty: true`. The evidence for a round about provenance was itself unattributable. |
| **How found** | **The provenance field, not a person.** Nobody read the deploy banner and nobody checked the tree; `deploy.tree_dirty` in the fetched record said so, and it said so because round 25 decided to carry the commit into the workspace through the deploy rather than write it afterwards from the fetching machine. The tree was committed as `ad936aa`, the lane redeployed, and the record that IS in this repository came from a clean tree. |
| **Why invisible** | Everything else was green. The deploy succeeded, the run succeeded, the close was correct, the digest matched, and the only thing wrong was that no commit contained the code that produced any of it. `tree_dirty` errs SAFE - it understates the evidence - which is exactly why it went eighteen rounds being wrong in the other direction without anybody chasing it, and exactly why nothing would have stopped this record being committed. |
| **Prevented by** | Two halves, and the split is the point. `scripts/databricks_run.sh fetch` now reads the record it just brought down and prints **DO NOT COMMIT THIS EVIDENCE**, why, and the three commands that produce a committable one. It does not die: by the time it can read the field the files are on disk, so dying prevents nothing, and it would skip the summary the step exists to print. The refusal lives where the commit is - `test_databricks_bundle.py::test_no_committed_evidence_came_from_a_deploy_that_was_not_a_commit` fails on committed evidence whose deploy was dirty **or unknown**, in `make fast`, in `make preflight` and in CI. Falsified in all three states, over both the record and the capture. |
| **The other half of it** | The command that published the good record was typed by hand - `databricks bundle run ... --only publish_evidence` - and a hand-typed `bundle run` goes straight past `require_fresh_deployment`. Legitimate that day, because a deploy came immediately before it; a hole in the guard on every other day. `step_run` takes a task selection now (`scripts/databricks_run.sh run publish_evidence`), so the convenient path is inside the guard rather than round the side of it, and a subcommand that takes no selection refuses one instead of ignoring it. |
| **Commits** | this round |

### A return whose order never arrived is counted in no column of the close

| | |
|---|---|
| **What** | A `return_registered` whose order line is not in the population gets `sale_ts IS NULL`, so `eligibility` marks it `return_without_order` and it never reaches `returns`; and the `rejected` CTE filters `AND sale_ts IS NOT NULL`, so it never reaches `returns_rejected_count` either. It is classified and then counted nowhere. **Measured: 3 such returns, and the published close says 22 rejected where 25 were classified.** |
| **How found** | By reading `gold_close.py` while reconciling the second close's return arithmetic. |
| **Why invisible** | It is not a filter bug that could be deleted. `rejected` groups by an accounting month derived from `sale_ts`, and a return with no sale has no month: a month-grouped close has nowhere to put it. Removing the filter would produce a NULL month group, which is worse. And ALL THREE lanes agree - `databricks/src/gold_close.py`, `src/samegold/oracle/gold_revenue.sql` and `src/samegold/pipelines/transform.py` - so no differential test can see it. This is the blind spot the README names in the witness table, with an instance. |
| **Not fixed on purpose** | A `COALESCE` would put those returns in some month, and which month is a contract question nobody has answered: the sale's month does not exist, the return's own month is a different quantity, and inventing one to make a total add up is how a close acquires revenue that no sale supports. It is documented in `CONTRACT.md`'s terms as a known gap, in README's "What is NOT claimed", and here. |
| **Commits** | this round |

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
| `deploy.tree_dirty` reached the record as the STRING `"false"`, so `if record["deploy"]["tree_dirty"]:` was true on a clean tree. | Three string-typed layers between the deploy and the record - a bundle variable, a job parameter, a notebook widget - and a string that reads like the value it is not. The same shape as the INT32 literal: a type crossing a boundary that does not carry types. | Converted at the source in `publish_evidence.py` to `True` / `False` / `None`, with `deploy.commit` as the discriminator for "unknown"; `test_a_boolean_in_the_record_is_a_boolean` walks every field of the record rather than the one that was wrong. | this round |
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
| **A comparison is only as current as the captured half it reads.** Without provenance a snapshot expires in silence, and the test stays green. | the dimension capture that could not name its run (`02adf05`) |
| **Provenance of the RUN is not provenance of the DATA.** Two files from one update can still describe two populations. | the parity comparison reporting 92 against 75 (this round) |
| **Evidence a reader cannot regenerate is not evidence.** | the late population produced in `/tmp` (this round) |
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
