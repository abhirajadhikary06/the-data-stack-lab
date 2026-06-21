import os
import polars as pl

from lakefs_client import compare_branches, commit, create_branch, download_object, merge_branches, upload_object
from quality.validity import validate_broadcasts
from transformation.clean import clean_broadcasts
from transformation.parquet import write_parquet
from transformation.aggregate import aggregate_broadcasts
from metadata.metadata import write_commit_metadata

def pipeline():
    # Creating Branch (feature-summary)
    branch = create_branch("lakefs-tutorial", "main", "feature-summary")

    # Downloading CSV file to data/raw
    local_csv = download_object(
        repo="lakefs-tutorial",
        branch="main",
        object_path="sb_dataset_v3.csv",
        destination="data/raw/sb_dataset_v3.csv",
    )

    # Adding check that downloaded object is not empty
    if os.path.getsize(local_csv) == 0:
        raise ValueError("Downloaded CSV file is empty. Please check the source object in lakeFS.")
    else:
        # Cleaning and validating the data
        df = pl.read_csv(local_csv)
        df = clean_broadcasts(df)
        df = validate_broadcasts(df)

        if df.shape[0] == 0:
            raise ValueError("No valid broadcast rows remained after cleaning and validation.")

        # Aggregation summary
        summary = aggregate_broadcasts(df)

        # Writing to destination
        output_path = "data/processed/sb_dataset_v3_summary.parquet"
        write_parquet(summary, output_path)

        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Expected parquet output was not created: {output_path}")

        with open(output_path, "rb") as f:
            file_content = f.read()

        # Upload Object
        upload_object(
            repo = "lakefs-tutorial",
            branch = branch,
            object_path = "sb_dataset_v3_summary.parquet",
            file_content = file_content
        )

        # Compare Branches
        comparison = compare_branches(
            repo = "lakefs-tutorial",
            source_branch = branch,
            destination_branch = "main"
        )

        diff_results = comparison.get("results", [])
        diff_types = {item.get("type") for item in diff_results if isinstance(item, dict)}

        # Listing down the difference between the branches
        if not diff_results:
            print("No differences found between branches. Skipping merge.")
        else:
            print(f"Differences found: {len(diff_results)} changes ({', '.join(sorted(diff_types))}). Proceeding with merge.")
            # Commit only when lakeFS sees staged object changes.
            commit_operation = commit(
                repo = "lakefs-tutorial",
                branch = branch,
                message = input("Enter commit message: ")
            )

            if commit_operation:
                commit_id = commit_operation["id"]
                commit_message = commit_operation["message"]
                commit_branch = branch

                write_commit_metadata(commit_id, commit_message, commit_branch)

            # Merge Branches with main branch
            merge_branches(
                repo = "lakefs-tutorial",
                source_branch = branch,
                destination_branch = "main"
            )


if __name__ == "__main__":
    pipeline()

