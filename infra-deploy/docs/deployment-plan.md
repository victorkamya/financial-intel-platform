# Piecemeal deployment plan: Steps 3-9 (Kinesis → Secrets → IAM → DR → CloudWatch → Databricks)

## Context
`deploy.py` provisions infrastructure in discrete, independently-verifiable
steps rather than one large apply: Step 1 (config) and Step 2 (5 S3 buckets),
followed by Kinesis stream, Secrets Manager secret, two IAM roles, DR buckets
+ replication, CloudWatch dashboard/alarm, and the Databricks
catalogs/schemas/groups. Each step is implemented, run, and confirmed via
`docs/VERIFICATION.md` before moving to the next. This avoids Terraform's
single-dependency-graph failure mode (a break in one resource no longer
blocks everything already working) and keeps progress visible in short,
verifiable increments.

Every code block below assumes `<prefix>` = the value in `outputs.json`
(`Get-Content outputs.json` to check — currently `financial-intel-platform-z7oxnm`).

## Structured logging

`outputs.json` gains a `"log"` array alongside the existing `"resources"`
dict: each entry is `{"ts": <ISO8601>, "step": "<name>", "event": "<what
happened>", "detail": "<...>"}`, appended via a `log_event()` helper next to
the existing `record_resource()`. This gives a timestamped audit trail of
every action taken (not just final resource names), useful for reconstructing
what happened if something fails partway through a step.

## Per-step blockers (beyond what's noted inline below)

- **Step 5 (IAM roles):** unlike buckets/stream/secret, role creation has no
  existence check yet — needs an `EntityAlreadyExists` catch before
  `create_role`, or a re-run will error instead of skipping.
- **Step 6 (DR replication):** a freshly-created IAM role can take a few
  seconds to propagate through AWS before S3 replication can assume it —
  `put_bucket_replication` immediately after role creation can transiently
  fail even with a correct policy. Needs a short retry/backoff, not a single
  attempt.
- **Step 8 (Databricks):** idempotency requires catching the specific
  "already exists" exception the installed `databricks-sdk` version raises
  for catalogs/schemas/groups — confirm the exact exception class against
  the installed SDK version before relying on it.

## Permissions required

No *new* grants are expected for Steps 5-9:
- AWS: `financial-intel-deploy`'s 5 managed policies (`AmazonS3FullAccess`,
  `AmazonKinesisFullAccess`, `SecretsManagerReadWrite`,
  `CloudWatchFullAccess`, `IAMFullAccess`) already cover Steps 5-7 (IAM role
  creation, DR buckets/replication, CloudWatch).
  `IAMFullAccess` also covers any incidental service-linked role creation
  S3 replication might trigger.
- Databricks: `overarching-sp`'s workspace membership + metastore admin
  status already covers Step 8 (catalog/schema/group creation).

If something still fails with `AccessDenied`/`PERMISSION_DENIED` despite
this, it's a named gap to flag explicitly rather than a generic permissions
sweep to guess at.

## Design notes

- **Grants + cluster policy:** deferred by design — Step 8 creates groups
  with no privileges, and the cluster policy resource stays out of scope
  entirely, matching the original design intent (both were left unimplemented
  there too).

## Every new terminal — run this first

```powershell
cd infra-deploy
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
. .\set-env.ps1
Get-ChildItem Env: | Where-Object Name -in @("AWS_ACCESS_KEY_ID","AWS_SECRET_ACCESS_KEY","DATABRICKS_HOST","DATABRICKS_CLIENT_ID","DATABRICKS_CLIENT_SECRET","UC_TRUST_ACCOUNT_ID","DATABRICKS_METASTORE_ID")
```
Expect all 7 names listed. Don't proceed to any step below until they are.

---

## Step 3 — Kinesis stream

**Run:**
```powershell
.\venv\Scripts\python.exe deploy.py
```

**Console output to look for:**
```
--- Step 3: Kinesis stream ---
  [ok]   created stream: <prefix>-market-data
```
(or `[skip] stream already exists` on a re-run). Watch out for
`AccessDenied` (IAM user missing `AmazonKinesisFullAccess`) or
`LimitExceededException` (shard limit on the account).

**Verify:**
```powershell
aws kinesis describe-stream-summary --stream-name <prefix>-market-data --region us-west-2
```
Look for `"StreamStatus": "ACTIVE"`, `"OpenShardCount": 2`,
`"RetentionPeriodHours": 24`.

---

## Step 4 — Secrets Manager

**Run:** same command as Step 3.

**Console output to look for:**
```
--- Step 4: Secrets Manager ---
  [ok]   created secret: <prefix>/sec-edgar-user-agent
```
Watch out for `AccessDeniedException` (missing `SecretsManagerReadWrite`) or
a secret stuck in a pending-deletion state from a prior failed run
(`InvalidRequestException: ... scheduled for deletion` — if seen, restore it
first with `aws secretsmanager restore-secret --secret-id <prefix>/sec-edgar-user-agent`).

**Verify:**
```powershell
aws secretsmanager describe-secret --secret-id <prefix>/sec-edgar-user-agent --region us-west-2
```
Look for `"RecoveryWindowInDays": 0` and no `DeletedDate` field present.

---

## Step 5 — IAM roles

**Run:** same command.

**Console output to look for:**
```
--- Step 5: IAM roles ---
  [ok]   created role: <prefix>-unity-catalog-role
  [ok]   created role: <prefix>-s3-replication-role
```
Watch out for `MalformedPolicyDocument` (typo in the trust/inline policy
JSON — this is a code bug, flag it) or `AccessDenied` (missing
`IAMFullAccess`).

**Verify:**
```powershell
aws iam get-role --role-name <prefix>-unity-catalog-role
aws iam list-role-policies --role-name <prefix>-unity-catalog-role
aws iam get-role-policy --role-name <prefix>-unity-catalog-role --policy-name <name-from-previous-command>
```
In the trust policy, confirm `Condition.StringEquals["sts:ExternalId"]`
exactly matches your `UC_TRUST_ACCOUNT_ID` value (the Databricks account ID).
In the inline policy, confirm all 5 medallion bucket ARNs from Step 2 are
listed.

---

## Step 6 — DR buckets + replication

**Run:** same command.

**Console output to look for:**
```
--- Step 6: DR buckets + replication ---
  [ok]   created bucket: <prefix>-bronze-dr
  [ok]   created bucket: <prefix>-gold-dr
  [ok]   replication configured: bronze -> bronze-dr
  [ok]   replication configured: gold -> gold-dr
```
Watch out for `InvalidRequest`/`ReplicationConfigurationNotFoundError`-style
errors — replication requires versioning enabled on **both** the source and
destination bucket first; if this fails, check versioning landed on the DR
buckets before the replication call ran.

**Verify:**
```powershell
aws s3api head-bucket --bucket <prefix>-bronze-dr --region us-east-1
aws s3api get-bucket-versioning --bucket <prefix>-bronze-dr --region us-east-1
aws s3api get-bucket-replication --bucket <prefix>-bronze --region us-west-2
```
Look for versioning `"Status": "Enabled"` on the DR bucket, and a
replication rule named `bronze-to-dr` with `"Status": "Enabled"`,
destination pointing at the `-bronze-dr` bucket, storage class
`STANDARD_IA`.

---

## Step 7 — CloudWatch

**Run:** same command.

**Console output to look for:**
```
--- Step 7: CloudWatch ---
  [ok]   dashboard created: <prefix>-ops
  [ok]   alarm created: <prefix>-kinesis-lag-alarm
```
Watch out for `InvalidParameterInput` (malformed dashboard JSON body — code
bug, flag it).

**Verify:**
```powershell
aws cloudwatch get-dashboard --dashboard-name <prefix>-ops --region us-west-2
aws cloudwatch describe-alarms --alarm-names <prefix>-kinesis-lag-alarm --region us-west-2
```
Look for a valid `DashboardBody` JSON payload returned, and the alarm listed
with `StateValue` of `INSUFFICIENT_DATA` (expected — no stream traffic yet,
not an error) or `OK`.

---

## Step 8 — Databricks catalogs/schemas/groups

**Run:** same command.

**Console output to look for:**
```
--- Step 8: Databricks catalogs/schemas/groups ---
  [ok]   catalog created: bronze
  [ok]   catalog created: silver
  [ok]   catalog created: gold
  [ok]   catalog created: ml
  [ok]   schema created: bronze.market_data
  [ok]   schema created: silver.market_data
  [ok]   schema created: gold.market_data
  [ok]   schema created: ml.ml_artifacts
  [ok]   group created: financial-intel-engineers
  [ok]   group created: financial-intel-analysts
```
Watch out for `PERMISSION_DENIED` — this is the same metastore
`CREATE CATALOG` grant issue hit earlier; if it recurs, re-check
`overarching-sp`'s privileges on the metastore. `RESOURCE_ALREADY_EXISTS`
should be caught and skipped, not fatal.

**Verify:**
```powershell
databricks catalogs list
databricks schemas list bronze
databricks schemas list ml
databricks groups list
```
Look for all 4 catalogs, `market_data`/`ml_artifacts` schemas in the right
catalogs, and both group display names.

---

## Step 9 — Wrap-up

**Run:**
```powershell
Get-Content outputs.json
```
Look for every resource key populated (bucket names, stream ARN, secret ARN,
role ARNs, DR bucket names, dashboard/alarm names) with no `null` or missing
entries — confirms every prior step actually recorded its output. Then flip
the matching ⏳ markers to ✅ in `docs/VERIFICATION.md`, filling in the real
names/ARNs shown here.

## Files touched
- `infra-deploy/deploy.py` — one function + `__main__` wiring per step.
- `infra-deploy/docs/VERIFICATION.md` — status flips as each step is confirmed.
- `infra-deploy/outputs.json` — auto-updated by the script, no manual edits.
