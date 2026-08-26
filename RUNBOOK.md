# Runbook: gated deployment

Operating procedure for deploying the `basic` pipeline through both approval
gates:

PR review → **approved deploy** → **STOPPED** → **approved start** → **RUNNING** → **output observed**

Two gates, each on the thing it is good at. A pull request gates the *SQL*,
because there is a diff to review. A GitHub Environment reviewer gates the
*activation*, because starting a statement is an operational act with no diff.
Nothing consumes data until a human approves twice.

Section 5 is the load-bearing verification: data sits unprocessed in the input
topic while the statement is stopped, and only flows once someone explicitly
releases it.

## Conventions

Every `<...>` value below is a placeholder for your own environment. Substitute
throughout, or set the shell variables in 0.1 once.

| Placeholder | Meaning |
|---|---|
| `<org>/<repo>` | The GitHub repository hosting this code |
| `<your-environment-id>` | Confluent environment, e.g. `env-xxxxxx` |
| `<your-cluster-id>` | Kafka cluster, e.g. `lkc-xxxxxxx` |
| `<your-sr-cluster-id>` | Schema Registry cluster, e.g. `lsrc-xxxxxxx` |
| `<your-compute-pool-id>` | Flink compute pool, e.g. `lfcp-xxxxxxx` |
| `<your-service-account-id>` | Statement principal, e.g. `sa-xxxxxxx` |
| `<your-flink-key-id>` | Flink API key ID (the secret is never written down) |

## Two paths through this document

| | Sections |
|---|---|
| **Gated path** — GitHub Actions + adapter, both approvals | 0 → 7, then 9 |
| **Local path** — adapter only, no gates | 0, 1, 2, then **8** in place of 3, 4 and 6 |

Section 8 drives the same adapter commands straight from a shell. It is the
fallback when Actions is unavailable, and useful for iterating on SQL. It
exercises the adapter but not the approval controls, so it is not a substitute
for the gated path.

---

## 0. One-time setup

### 0.1 Shell variables

```bash
export ENV_ID=<your-environment-id>
export CLUSTER=<your-cluster-id>
export SR_CLUSTER=<your-sr-cluster-id>
export CLOUD=aws
export REGION=us-east-2
export STATEMENT=demo-basic-pipeline
```

### 0.2 Configuration

`config/demo-dev.yaml` ships with placeholder identifiers. Either edit it in
place, or copy it to `config/demo-dev.local.yaml` (gitignored) and pass that
path to `--config` to keep live identifiers out of version control.

### 0.3 Flink credentials (used by the adapter)

The Flink API key must be of type `flink-region`, scoped to the same cloud and
region as the compute pool, and owned by the statement principal:

```bash
confluent api-key create --resource flink --cloud $CLOUD --region $REGION \
  --environment $ENV_ID --service-account <your-service-account-id>
```

```bash
export FLINK_API_KEY=<your-flink-key-id>
read -rs FLINK_API_SECRET && export FLINK_API_SECRET
```

`read -rs` keeps the secret out of shell history. The adapter reads both from
the environment and never logs them.

### 0.4 Kafka and Schema Registry credentials (used to produce and observe)

These are **not** the Flink credentials. They are only needed for the manual
produce/consume verification in sections 5 and 7:

```bash
confluent api-key create --resource $CLUSTER --environment $ENV_ID
confluent api-key create --resource $SR_CLUSTER --environment $ENV_ID
```

```bash
export KAFKA_KEY=...       KAFKA_SECRET=...
export SR_KEY=...          SR_SECRET=...
```

### 0.5 RBAC

The statement principal needs:

| Role | Scope |
|---|---|
| FlinkDeveloper | environment `$ENV_ID` and pool `<your-compute-pool-id>` |
| Assigner | `ServiceAccount:<your-service-account-id>` |
| DeveloperRead / DeveloperWrite | all `flink-demo-*` topics and subjects |
| DeveloperManage | `flink-demo-` prefixed topics |

Verify with:

```bash
confluent iam rbac role-binding list \
  --principal User:<your-service-account-id> --inclusive
```

`--inclusive` matters — without it, compute-pool-scoped bindings are invisible.

### 0.6 GitHub environment and approval gate

Create a `demo-dev` environment with at least one required reviewer. This is
what turns the `environment: demo-dev` line in `deploy.yml` into an approval
gate. **Without a reviewer on the environment, that line does nothing and every
dispatch deploys immediately.** Verify:

```bash
gh api repos/<org>/<repo>/environments/demo-dev \
  --jq '.protection_rules[] | select(.type=="required_reviewers")
        | {reviewers: [.reviewers[].reviewer.login], prevent_self_review}'
```

Then add the credentials as **environment** secrets. Both are required or the
deploy job fails at the adapter:

```bash
gh secret set FLINK_API_KEY --env demo-dev --body '<your-flink-key-id>'
gh secret set FLINK_API_SECRET --env demo-dev    # prompts, stays out of history
gh secret list --env demo-dev                    # both must appear
```

They must be environment secrets (`--env demo-dev`), not repository secrets. A
repository secret is readable by any workflow whether or not it passed the gate,
which would make the approval decorative.

> Environment protection rules are free on public repositories. On a private
> repository they require GitHub Pro, Team, or Enterprise.

---

## 1. Reset to a clean slate

`create-stopped` is idempotent: if the statement already exists it reports it
and creates nothing. To exercise the full creation path, delete it first.

```bash
confluent flink statement delete $STATEMENT \
  --environment $ENV_ID --cloud $CLOUD --region $REGION
```

Confirm it is gone:

```bash
confluent flink statement list --environment $ENV_ID --cloud $CLOUD --region $REGION \
  | grep $STATEMENT || echo "clean"
```

> Deleting drops the statement and its processing state. That is harmless for
> this example pipeline; on a production pipeline it loses offsets and any
> accumulated state. Prefer `stop`/`resume` (section 9).

---

## 2. Gate one: pull request review

Run the pre-flight checks locally first:

```bash
python -m adapter.cli validate \
  --pipeline basic \
  --config config/demo-dev.yaml \
  --require-credentials
```

**Expect:** every check `PASS`, `Result: OK`. This makes no API calls.

Then open a pull request. `validate.yml` runs on `pull_request` and executes the
same command without `--require-credentials` — secrets are not exposed to fork
pull requests, so the check warns on absent credentials rather than failing.

```bash
git checkout -b change/pipeline-update
# edit sql/basic/pipeline.sql or config/demo-dev.yaml
git commit -am "Adjust basic pipeline"
git push -u origin change/pipeline-update
gh pr create --fill
gh pr checks --watch
```

**Expect:** `Validate Flink pipeline` green on the pull request.

### Confirming the check fails closed

A guardrail is only worth having if you have seen it reject something. To
confirm the PR check is actually evaluating the configuration rather than
passing unconditionally, introduce a known-invalid value on a scratch branch:

```bash
# statement_name must match ^[a-z][a-z0-9-]{0,99}$ — an underscore is invalid
sed -i '' 's/statement_name: demo-basic-pipeline/statement_name: Demo_Basic_Pipeline/' config/demo-dev.yaml
git commit -am "Temporary: verify validation rejects an invalid statement name"
git push
gh pr checks --watch          # expect red
git revert --no-edit HEAD && git push
gh pr checks --watch          # expect green
```

**Merge the pull request.** Merging deploys nothing on its own — the second gate
is separate and deliberate.

```bash
gh pr merge --squash --delete-branch
git checkout main && git pull
```

---

## 3. Gate two: approved deploy → STOPPED

Dispatch the deployment. It will **not** run yet.

```bash
gh workflow run deploy.yml -f operation=create-stopped -f pipeline=basic
gh run list --workflow=deploy.yml --limit 1
```

**Expect:** status `waiting`. The job is held at the `demo-dev` environment
pending review; the run displays *"Waiting for review"* with a **Review
deployments** button in the Actions tab.

Approve in the UI, or from the CLI:

```bash
RUN_ID=$(gh run list --workflow=deploy.yml --limit 1 --json databaseId --jq '.[0].databaseId')
gh api -X POST repos/<org>/<repo>/actions/runs/$RUN_ID/deployment_protection_rule \
  -f environment_name=demo-dev -f state=approved -f comment='Approved'
gh run watch $RUN_ID
```

**Expect** in the job log:

```
Validation passed for pipeline 'basic'
Creating statement 'demo-basic-pipeline'
Requesting spec.stopped=true (the API starts statements on create)
Polling for STOPPED (timeout 300s)
  phase: PENDING
  phase: RUNNING
  phase: STOPPED

Action: create-stopped
Phase: STOPPED
Stopped flag: True
Processing started: no
Note: Created and then stopped: the API starts statements on create, so the
      statement was briefly live before spec.stopped=true was applied.
```

**Seeing `RUNNING` in the middle is expected and correct.** Confluent Cloud has
no atomic create-stopped: the API starts every statement on creation and ignores
`spec.stopped` in the POST body. The adapter immediately follows with
`PATCH /spec/stopped`. Treat this as "not left running", not as a guarantee that
nothing was ever read.

---

## 4. Confirm STOPPED, and collect the audit trail

Confirm from outside the adapter, so the adapter's own report is not the only
evidence:

```bash
confluent flink statement describe $STATEMENT \
  --environment $ENV_ID --cloud $CLOUD --region $REGION
```

**Checkpoint — do not continue until `Status: STOPPED`.**

The run published its deployment manifest as an artifact, retained 90 days:

```bash
gh run download $RUN_ID --dir /tmp/manifest && cat /tmp/manifest/*/*.json
```

The manifest records the SQL hash, release hash, git commit, compute pool and
principal for exactly what was deployed; the run that produced it carries the
approver's identity. Together they answer what shipped and who authorised it.

---

## 5. Verify the gate holds: produce while stopped

Record the output topic's current depth, so new records are distinguishable from
existing ones:

```bash
confluent kafka topic describe flink-demo-output --cluster $CLUSTER --environment $ENV_ID
```

Produce three records into the input topic:

```bash
cat <<EOF | confluent kafka topic produce flink-demo-input \
  --cluster $CLUSTER --environment $ENV_ID \
  --api-key $KAFKA_KEY --api-secret $KAFKA_SECRET \
  --schema-registry-api-key $SR_KEY --schema-registry-api-secret $SR_SECRET \
  --value-format avro --schema schemas/basic/input.avsc
{"event_id":"gated-1","event_type":"demo","payload":{"string":"before start"},"event_ts":$(date +%s)000}
{"event_id":"gated-2","event_type":"demo","payload":{"string":"before start"},"event_ts":$(date +%s)000}
{"event_id":"gated-3","event_type":"demo","payload":{"string":"before start"},"event_ts":$(date +%s)000}
EOF
```

> `payload` is a `["null","string"]` union, so Avro JSON encoding requires the
> `{"string":"..."}` wrapper. A bare `"before start"` will be rejected. Use
> `null` for an absent payload.

Confirm nothing moved:

```bash
timeout 20 confluent kafka topic consume flink-demo-output \
  --cluster $CLUSTER --environment $ENV_ID \
  --api-key $KAFKA_KEY --api-secret $KAFKA_SECRET \
  --schema-registry-api-key $SR_KEY --schema-registry-api-secret $SR_SECRET \
  --value-format avro --from-beginning
```

**Expect:** no records containing `gated-1/2/3`. This is the property the whole
design exists to provide — the input topic has data, the statement is fully
deployed, and nothing is being processed because activation was not approved.

Confirm the adapter agrees:

```bash
python -m adapter.cli status --pipeline basic --config config/demo-dev.yaml
```

**Expect:** `Phase: STOPPED`, `Processing started: no`.

---

## 6. Approved start → RUNNING

The second approval, and the one that lets data move.

```bash
gh workflow run deploy.yml -f operation=start -f pipeline=basic
sleep 5
RUN_ID=$(gh run list --workflow=deploy.yml --limit 1 --json databaseId --jq '.[0].databaseId')
gh api -X POST repos/<org>/<repo>/actions/runs/$RUN_ID/deployment_protection_rule \
  -f environment_name=demo-dev -f state=approved -f comment='Release approved'
gh run watch $RUN_ID
```

**Expect** in the job log:

```
Setting spec.stopped=false on 'demo-basic-pipeline'
Polling for RUNNING (timeout 300s)
  phase: RUNNING

Phase: RUNNING
Stopped flag: False
Processing started: yes
```

`start` also verifies the deployed SQL hash against the local release and
refuses to proceed on a mismatch, because submitted SQL is immutable. Override
with `--allow-sql-mismatch` only when the reason for the difference is
understood.

At this point: the SQL was reviewed on a pull request, the deployment was
approved by a named person, the statement existed without touching data, and a
second named approval released it. Each step is recorded in the Actions log.

---

## 7. Observe output

Confluent Cloud Flink sources default to reading from the earliest offset, so
the three records produced while stopped should now flow through:

```bash
confluent kafka topic consume flink-demo-output \
  --cluster $CLUSTER --environment $ENV_ID \
  --api-key $KAFKA_KEY --api-secret $KAFKA_SECRET \
  --schema-registry-api-key $SR_KEY --schema-registry-api-secret $SR_SECRET \
  --value-format avro --from-beginning
```

**Expect** `gated-1`, `gated-2`, `gated-3` within roughly 30–60s of the statement
reaching RUNNING.

If they do not appear, the source may be configured for latest-offset. Produce
fresh records with the consumer still attached:

```bash
cat <<EOF | confluent kafka topic produce flink-demo-input \
  --cluster $CLUSTER --environment $ENV_ID \
  --api-key $KAFKA_KEY --api-secret $KAFKA_SECRET \
  --schema-registry-api-key $SR_KEY --schema-registry-api-secret $SR_SECRET \
  --value-format avro --schema schemas/basic/input.avsc
{"event_id":"live-1","event_type":"demo","payload":{"string":"after start"},"event_ts":$(date +%s)000}
EOF
```

`live-1` appearing downstream confirms the pipeline end to end either way.

Cross-check that the statement is healthy rather than silently failing:

```bash
confluent flink statement describe $STATEMENT \
  --environment $ENV_ID --cloud $CLOUD --region $REGION
confluent flink statement exception list $STATEMENT \
  --environment $ENV_ID --cloud $CLOUD --region $REGION
```

---

## 8. Local CLI path (ungated)

Substitutes for sections 3, 4 and 6. Same adapter and same Confluent calls, but
no approval gate and no run-level audit trail.

```bash
# in place of sections 3 and 4
python -m adapter.cli create-stopped --pipeline basic --config config/demo-dev.yaml
confluent flink statement describe $STATEMENT \
  --environment $ENV_ID --cloud $CLOUD --region $REGION      # expect STOPPED

# then run section 5 to verify the gate, then:

# in place of section 6
python -m adapter.cli start --pipeline basic --config config/demo-dev.yaml
```

Manifests land in `manifests/` locally rather than as run artifacts.

Requires `FLINK_API_KEY` / `FLINK_API_SECRET` exported in your shell (0.3). If
they are only set as GitHub environment secrets, these commands fail with
`FLINK_API_KEY and FLINK_API_SECRET must be set in the environment`.

---

## 9. Stop and reset

```bash
gh workflow run deploy.yml -f operation=stop -f pipeline=basic
# approve as in section 6
```

or locally:

```bash
python -m adapter.cli stop --pipeline basic --config config/demo-dev.yaml
```

**Expect:** `Phase: STOPPED`, `Processing started: no`.

`resume` restarts the same statement, preserving its identity and state.
Deleting and recreating does not — use `stop`/`resume` for pause-and-continue,
and reserve delete for a genuine reset (section 1).

---

## Promoting this to a production environment

The sections above describe a single environment. Three changes make the pattern
production-grade, none of which alter `deploy.yml`:

1. **A separate reviewer.** Enable *Prevent self-review* and name reviewers other
   than the people who dispatch, so approval is genuinely a second pair of eyes.
2. **A branch policy.** Set `deployment_branch_policy` to `main` only, so an
   unmerged branch cannot deploy even with an approval.
3. **One environment per stage** — `dev`, `stage`, `prod` — each with its own
   secrets, its own reviewers, and its own service-account principal.

That the workflow file does not change is the point: the gate is configuration,
so promoting the pattern is a policy decision rather than an engineering task.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `403` on any statement call | Principal lacks environment-scoped `FlinkDeveloper` | `confluent iam rbac role-binding create --principal User:<your-service-account-id> --role FlinkDeveloper --environment $ENV_ID` |
| `403` only on create | Missing `Assigner` on the service account | `--role Assigner --resource ServiceAccount:<your-service-account-id>` |
| `401` | Wrong or expired key | Key must be `flink-region` type for the pool's cloud and region |
| `create-stopped` reports "already exists" | Statement present | Delete it (section 1) or use `status` |
| Stuck in `RUNNING` while waiting for STOPPED | Stop request accepted but ineffective | The adapter fails after 45s with an explicit message |
| `Statement ... does not exist` on `start` | Never created, or deleted | Run `create-stopped` first |
| SQL hash mismatch on `start` | Local SQL differs from deployed | Deploy under a new statement name; submitted SQL is immutable |
| Produce rejected on `payload` | Avro union needs wrapping | `{"string":"..."}` or `null` |
| No output after `start` | Source startup offset, or a runtime failure | Produce fresh records; check `statement exception list` |
| Dispatch runs immediately, no approval | Reviewer missing from the environment | Add a required reviewer per 0.6 |
| Deploy job fails on missing credentials | Secrets absent or set at repository scope | `gh secret list --env demo-dev` — both must be **environment** secrets |
| Approval API returns `404` | Wrong run ID, or the run is not waiting | Re-read `RUN_ID`; only `waiting` runs accept approval |

## Known limitations

- `tests/` and `scripts/` contain empty placeholder files. `validate.yml`
  tolerates the absent suite via an exit-code-5 guard (`pytest -q || [ $? -eq 5 ]`);
  remove that guard once real tests exist.
- There is an unavoidable window during `create-stopped` where the statement is
  live, because Confluent exposes no atomic create-stopped primitive (section 3).
- Merging a pull request deploys nothing by design. If merge-triggered
  `create-stopped` is wanted later, add a `push: branches: [main]` job — but note
  that every merge would then deploy.
