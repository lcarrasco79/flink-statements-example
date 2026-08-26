# flink-statements-example

A reference implementation of **gated CI/CD for Confluent Cloud Flink
statements**. It shows how to put two independent human approvals in front of a
streaming pipeline: one on the SQL, and one on the moment data starts moving.

```
PR review  →  approved deploy  →  STOPPED  →  approved start  →  RUNNING
   ↑                                              ↑
gates the SQL (there is a diff)          gates activation (there is no diff)
```

The two gates exist because they guard different things. A pull request is the
right control for a SQL change, because reviewers have a diff to read. It is the
wrong control for *activation* — starting a statement is an operational act with
no diff at all. So a statement is created in a `STOPPED` state, and a separate
GitHub Environment approval releases it. Between those two points the statement
exists, is fully deployed, and consumes nothing.

## What's in here

| Path | Purpose |
|---|---|
| `adapter/` | Python adapter over the Confluent Cloud Flink Statements REST API |
| `adapter/cli.py` | `validate`, `create-stopped`, `status`, `start`, `stop`, `resume`, `manifest` |
| `adapter/flink_statements.py` | Config loading, validation, SQL hashing, REST client, polling, manifests |
| `config/demo-dev.yaml` | Environment configuration (placeholder identifiers — edit before use) |
| `sql/basic/pipeline.sql` | Pass-through pipeline: one input topic to one output topic |
| `sql/stateful/pipeline.sql` | Interval join across two input topics |
| `schemas/` | Avro schemas for the pipeline topics |
| `.github/workflows/validate.yml` | Read-only PR check. Never calls Confluent |
| `.github/workflows/deploy.yml` | Gated lifecycle operations, behind an Environment approval |
| `manifests/` | Deployment manifests written per operation (gitignored) |
| `RUNBOOK.md` | Step-by-step operating procedure |

## Design points worth knowing

**Credentials never touch the repository.** The adapter reads `FLINK_API_KEY`
and `FLINK_API_SECRET` from the environment, and never logs them. In CI they are
**environment** secrets scoped to `demo-dev`, not repository secrets — a
repository secret is readable by any workflow whether or not it passed the gate,
which would make the approval decorative.

**Every operation writes a deployment manifest** to `manifests/`, recording the
SQL hash, the release hash, the git commit, the compute pool, the principal, and
the resulting phase. The deploy workflow uploads it as a run artifact with 90-day
retention, so what shipped is recorded alongside who approved it.

**`start` verifies the deployed SQL against the local release** and refuses to
proceed on a hash mismatch, because submitted Flink SQL is immutable — a
statement whose SQL differs from the repository is not the statement that was
reviewed.

**`create-stopped` has an unavoidable live window.** Confluent Cloud starts every
statement on creation and ignores `spec.stopped` in the POST body, so the adapter
must follow immediately with `PATCH /spec/stopped`. Seeing a brief `RUNNING`
phase during `create-stopped` is expected. Treat the result as "not left
running", not as a guarantee that nothing was ever read.

**The adapter never creates or deletes topics.** `policy.allow_topic_create` and
`policy.allow_topic_delete` must both be `false`, and validation fails if they
are not. Topic lifecycle is deliberately outside this tool's authority.

## Quick start

```bash
pip install -r requirements.txt

# 1. Point the config at your own environment
#    (or copy to config/demo-dev.local.yaml, which is gitignored)
$EDITOR config/demo-dev.yaml

# 2. Pre-flight checks — makes no API calls, changes nothing
python -m adapter.cli validate --pipeline basic --config config/demo-dev.yaml

# 3. Credentials for the live operations below
export FLINK_API_KEY=<your-flink-key-id>
read -rs FLINK_API_SECRET && export FLINK_API_SECRET

# 4. Deploy stopped, inspect, then release
python -m adapter.cli create-stopped --pipeline basic --config config/demo-dev.yaml
python -m adapter.cli status         --pipeline basic --config config/demo-dev.yaml
python -m adapter.cli start          --pipeline basic --config config/demo-dev.yaml
```

`RUNBOOK.md` covers the full gated path through GitHub Actions, including how to
set up the approval gate, verify the statement is genuinely idle while stopped,
and promote the pattern to multiple environments.

## Requirements

- Python 3.12
- `requests`, `PyYAML` (see `requirements.txt`)
- Confluent CLI, for the out-of-band verification steps in the runbook
- A Confluent Cloud environment with a Flink compute pool and a service account
  holding `FlinkDeveloper` (environment- and pool-scoped) plus `Assigner` on
  itself

## Current limitations

- `tests/` and `scripts/` contain empty placeholder files. `validate.yml`
  tolerates the absent suite with an exit-code-5 guard (`pytest -q || [ $? -eq 5 ]`);
  remove that guard once real tests exist.
- The brief live window during `create-stopped` described above.
- Merging a pull request deploys nothing by design — deployment is always an
  explicit, approved `workflow_dispatch`.
