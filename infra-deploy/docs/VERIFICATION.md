# Deployment Verification Checklist

Legend: ✅ deployed — verifiable now &nbsp;|&nbsp; ⚠️ partial &nbsp;|&nbsp; ⏳ not deployed

All commands assume you're in `infra-deploy/` with AWS/Databricks env vars set
(see `set-env.ps1`). Replace `<prefix>` with the value from `outputs.json`
and `<account-id>` with your own AWS account ID.

## 0. Load current resource names

```powershell
Get-Content outputs.json
```

---

## 1. ✅ S3 Medallion Buckets (deployed)

Run for each of: `bronze`, `silver`, `gold`, `mlflow`, `unity-catalog`.

**Existence:**
```powershell
aws s3api head-bucket --bucket <prefix>-bronze
```
No output + exit code 0 = exists.

**Versioning** (Enabled on bronze/silver/gold; empty response on mlflow/unity-catalog):
```powershell
aws s3api get-bucket-versioning --bucket <prefix>-bronze
```

**Public access block** (all four flags `true`, all 5 buckets):
```powershell
aws s3api get-public-access-block --bucket <prefix>-bronze
```

**Tags** (`Project`, `Environment`, `ManagedBy=python-script`, `Owner`):
```powershell
aws s3api get-bucket-tagging --bucket <prefix>-bronze
```

**Region** (expect `us-west-2`):
```powershell
aws s3api get-bucket-location --bucket <prefix>-bronze
```

**Quick combined listing:**
```powershell
aws s3 ls | Select-String "<prefix>"
```

---

## 2. ✅ Kinesis Stream (deployed)

```powershell
aws kinesis describe-stream-summary --stream-name <prefix>-market-data --region us-west-2
```
Expect: `StreamStatus=ACTIVE`, `OpenShardCount=2`, `RetentionPeriodHours=24`.

ARN: `arn:aws:kinesis:us-west-2:<account-id>:stream/<prefix>-market-data`

---

## 3. ✅ Secrets Manager (deployed)

```powershell
aws secretsmanager describe-secret --secret-id <prefix>/sec-edgar-user-agent --region us-west-2
```
Expect the secret to exist. ARN:
`arn:aws:secretsmanager:us-west-2:<account-id>:secret:<prefix>/sec-edgar-user-agent-wxFrYk`

---

## 4. ⚠️ IAM Roles (partial — see known gap below)

Only `s3-replication-role` was created:
```powershell
aws iam get-role --role-name <prefix>-s3-replication-role
aws iam list-role-policies --role-name <prefix>-s3-replication-role
```
ARN: `arn:aws:iam::<account-id>:role/<prefix>-s3-replication-role`

`unity-catalog-role` was **deliberately not created** — see "Known gaps" below.

---

## 5. ✅ DR Buckets + Replication (deployed)

```powershell
aws s3api head-bucket --bucket <prefix>-bronze-dr --region us-east-1
aws s3api get-bucket-versioning --bucket <prefix>-bronze-dr --region us-east-1
aws s3api get-bucket-replication --bucket <prefix>-bronze --region us-west-2
```
Expect replication rules `bronze-to-dr` / `gold-to-dr`, status `Enabled`,
storage class `STANDARD_IA`, `Priority: 1`.

---

## 6. ✅ CloudWatch (deployed)

```powershell
aws cloudwatch get-dashboard --dashboard-name <prefix>-ops --region us-west-2
aws cloudwatch describe-alarms --alarm-names <prefix>-kinesis-lag-alarm --region us-west-2
```

---

## 7. ⚠️ Databricks Catalogs / Schemas / Groups (partial — see known gap below)

Catalogs and schemas deployed via SQL (`CREATE CATALOG`/`CREATE SCHEMA`, using
Default Storage since the metastore has no metastore-level storage root):
```powershell
databricks catalogs list
databricks schemas list bronze     # expect: market_data
databricks schemas list silver     # expect: market_data
databricks schemas list gold       # expect: market_data
databricks schemas list ml         # expect: ml_artifacts
```

**Groups were not created** — `overarching-sp` has metastore admin but not
workspace admin, and the SCIM Groups API requires workspace admin
specifically. `financial-intel-engineers`/`financial-intel-analysts` do not
exist yet. To finish this: either grant `overarching-sp` workspace admin, or
create the two groups manually via Workspace Settings → Identity and access
→ Groups, then re-run `deploy.py` (it will detect and skip if already
present).

---

## ⚠️ Known gaps

1. **`unity-catalog-role` was never created.** Its trust policy needs a
   storage-credential-specific `ExternalId` that only exists after creating
   a Unity Catalog Storage Credential object — which was already deferred
   (catalogs/schemas use Default Storage / the metastore's default managed
   location, not these S3 buckets). Also, the original design's trust
   policy used `"Service": "databricks.amazonaws.com"` as the principal,
   which is invalid — Databricks isn't an AWS-owned service; cross-account
   role assumption uses Databricks' own AWS account
   (`arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL`)
   plus a self-assuming statement. If this integration is added later, both
   issues need fixing, not just the ExternalId.
2. **The two Databricks groups don't exist** (see Section 7) — needs
   workspace admin on `overarching-sp`, or manual creation.
3. **AWS↔Databricks storage integration**: even with catalogs/schemas
   deployed, they are **not backed by the S3 buckets from Section 1** —
   Unity Catalog storage credentials/external locations were never wired
   up. The buckets and the catalogs exist independently.
