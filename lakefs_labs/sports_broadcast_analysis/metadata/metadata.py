import markdown

def write_commit_metadata(commit_id, commit_message, commit_branch):
    markdown_test = f"""
    - **Commit ID:** {commit_id}
    - **Message:** {commit_message}
    - **Branch:** {commit_branch}
    """
    html = markdown.markdown(markdown_test)
    with open("metadata/COMMIT_METADATA.md", "a") as f:
        f.write(markdown_test + '\n')
    return {
        "message": f"{commit_id} has been written to metadata/COMMIT_METADATA.md",
        "html_preview": html
    }
