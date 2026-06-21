<img src="lakefs.gif" alt="LakeFS" width="220" align="left" />
LakeFS

LakeFS is an open-source data version control system that brings Git-like semantics — branches, commits, merges, and rollbacks — to object storage and data lakes.

<br clear="left" />

## Setup
- MinIO: Minio is running on port `9001`-> login with credentials -> Create a Bucket, assume name `lakefs-project`
- LakeFS: Login to LakeFS with credentials (ACCESS_KEY, SECRET_KEY) -> In place of bucket location put: `s3://lakefs-project/`

## Architecture

A LakeFS internals looks like:

```markdown
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
```

LakeFS stores the logical path (`sb_dataset.csv`) in Postgres, mapped to the actual bytes in MinIO (`data/<hash>/<hash>`).

## Core Concepts

**Repository** — The top-level namespace in LakeFS, similar to a Git repository (e.g. `lakefs-tutorial`).

**Object** — Any file stored in a repository, such as `sb_dataset.csv` or `analytics/sport_summary.parquet`. LakeFS treats all file formats as objects.

**Branch** — An isolated workspace for experimenting without touching production data. A new branch starts out pointing to the same snapshot as its source.

```python
def create_branch(repo, source, branch):
    response = requests.post(
        f"{BASE_URL}/api/v1/repositories/{repo}/branches",
        auth=AUTH,
        json={"source": source, "name": branch}
    )
    if response.status_code == 409:
        return branch
    response.raise_for_status()
    return branch

branch = create_branch("lakefs-tutorial", "main", "feature-cleaning")
```

**Commit** — A snapshot of every object on a branch at a point in time; a dataset version, not a code version.

```python
def commit(repo, branch, message):
    response = requests.post(
        f"{BASE_URL}/api/v1/repositories/{repo}/branches/{branch}/commits",
        auth=AUTH,
        json={"message": message, "allow_empty": False, "force": False}
    )
    response.raise_for_status()
    return response.json()

commit(repo="lakefs-tutorial", branch="feature-cleaning", message="Remove duplicate broadcasts")
```

**Merge** — Promotes validated changes from a feature branch into `main`.

```python
def merge_branches(repo, source_branch, destination_branch):
    response = requests.post(
        f"{BASE_URL}/api/v1/repositories/{repo}/refs/{source_branch}/merge/{destination_branch}",
        auth=AUTH,
    )
    response.raise_for_status()
    return response.json()

merge_branches(repo="lakefs-tutorial", source_branch="feature-cleaning", destination_branch="main")
```

**Compare** — Diffs two branches before merging, showing additions, modifications, and deletions, so promotions are reviewable rather than blind overwrites.

```python
def compare_branches(repo, source_branch, destination_branch):
    response = requests.get(
        f"{BASE_URL}/api/v1/repositories/{repo}/refs/{source_branch}/diff/{destination_branch}",
        auth=AUTH,
    )
    response.raise_for_status()
    return response.json()

diff = compare_branches(repo="lakefs-tutorial", source_branch="feature-cleaning", destination_branch="main")
```

## Working with Objects

Every read or write goes through a branch (or ref), which keeps changes isolated until they're committed and merged.

**Download**

```python
def download_object(repo, branch, object_path, destination):
    response = requests.get(
        f"{BASE_URL}/api/v1/repositories/{repo}/refs/{branch}/objects",
        auth=AUTH,
        params={"path": object_path},
        stream=True,
    )
    response.raise_for_status()

    dest_path = Path(destination)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    with open(dest_path, "wb") as fh:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                fh.write(chunk)

    return str(dest_path)
```

**Upload**

```python
def upload_object(repo, branch, object_path, file_content):
    response = requests.post(
        f"{BASE_URL}/api/v1/repositories/{repo}/branches/{branch}/objects",
        auth=AUTH,
        params={"path": object_path},
        files={"content": (Path(object_path).name, file_content)},
    )
    response.raise_for_status()
    return response.json()
```

**Delete**

```python
def delete_object(repo, branch, object_path):
    response = requests.delete(
        f"{BASE_URL}/api/v1/repositories/{repo}/branches/{branch}/objects",
        auth=AUTH,
        params={"path": object_path},
    )
    response.raise_for_status()
    return response.json()
```

## Example Pipeline

This flow branches off `main`, downloads a dataset, cleans and validates it with Polars, and writes the result to Parquet locally — stopping short of upload/commit/merge so the output can be inspected first.

```python
import polars as pl
from dotenv import load_dotenv
load_dotenv()

from lakefs_client import download_object, create_branch
from quality.validity import validate_broadcasts
from transformation.clean import clean_broadcasts
from transformation.parquet import write_parquet

branch = create_branch("lakefs-tutorial", "main", "feature-polars")

local_csv = download_object(
    repo="lakefs-tutorial",
    branch="main",
    object_path="sb_dataset_v3.csv",
    destination="data/raw/sb_dataset_v3.csv",
)

df = pl.read_csv(local_csv)
df = clean_broadcasts(df)
df = validate_broadcasts(df)

write_parquet(df, "data/processed/sb_dataset_v3_cleaned.parquet")
```

Cleaning runs before validation, so validation checks aren't tripped up by issues cleaning would have already fixed.

### Promoting the result

Once the transformed data looks good, push it back to the branch and promote it to `main`:

```python
from lakefs_client import upload_object, commit, compare_branches, merge_branches

with open("data/processed/sb_dataset_v3_cleaned.parquet", "rb") as f:
    file_content = f.read()

upload_object(repo="lakefs-tutorial", branch=branch, object_path="sb_dataset_v3_cleaned.parquet", file_content=file_content)
commit(repo="lakefs-tutorial", branch=branch, message="Add cleaned and validated broadcast dataset")
compare_branches(repo="lakefs-tutorial", source_branch=branch, destination_branch="main")
merge_branches(repo="lakefs-tutorial", source_branch=branch, destination_branch="main")
```

This is the same flow an orchestrator like Airflow would run on a schedule: create branch → read → clean → validate → upload → commit → compare → merge.

## Data Quality

Validate data before promoting it to `main`. Typical checks: drop null event dates, drop null durations, remove duplicate IDs, reject invalid durations, reject placeholder dates.

```python
df = validate_broadcasts(df)
```

Only data that passes validation should continue further down the pipeline.

## Data Lineage

Because every branch change happens through a commit, lineage comes for free — each dataset version traces back to its origin by walking the commit history.

```
Commit A → Raw dataset
Commit B → Duplicates removed
Commit C → Parquet generated
Commit D → Aggregation created
```

## Time Travel

Every commit is an immutable snapshot, so older data versions stay accessible even after `main` has moved forward. If `main` points to Commit D and that commit turns out to have an issue, you can roll back to Commit C without rebuilding the dataset from scratch.