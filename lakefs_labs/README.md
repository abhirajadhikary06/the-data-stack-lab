<img src="lakefs.gif" alt="LakeFS" width="220" align="left" />

## LakeFS
lakeFS is an open-source data version control system that brings Git-like semantics (branches, commits, merges, and rollbacks) to object storage and data lakes.

## Setup
- MinIO: Minio is running on port `9001`-> login with credentials -> Create a Bucket, assume name `lakefs-project`
- LakeFS: Login to LakeFS with credentials (ACCESS_KEY, SECRET_KEY) -> In place of bucket location put: `s3://lakefs-project/`

┌──────────────────────┐
│   User / Client      │
└─────────┬────────────┘
          │ Upload sb_dataset.csv
          v
┌──────────────────────┐
│     LakeFS Server    │
└─────────┬────────────┘
          │
          ├───────────────────────────────┐
          │                               │
          v                               v
┌──────────────────────┐        ┌──────────────────────┐
│   Postgres Database  │        │   MinIO Object Store │
│  (Metadata / Mapping)│        │ (Physical File Data) │
└─────────┬────────────┘        └─────────┬────────────┘
          │                               │
          │ Stores logical path           │ Stores actual bytes as:
          │ "sb_dataset.csv"              │ data/<hash>/<hash>
          v                               v
┌────────────────────────────────────────────────────────┐
│ Mapping Example:                                       │
│ sb_dataset.csv  →  data/a1b2c3d4/e5f6g7h8              │
└────────────────────────────────────────────────────────┘

