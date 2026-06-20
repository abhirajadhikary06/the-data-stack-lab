import requests
from dotenv import load_dotenv
load_dotenv()
import os
from pathlib import Path

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
BASE_URL = os.getenv("LAKEFS_ENDPOINT", "http://localhost:8000")

AUTH = (ACCESS_KEY, SECRET_KEY)

# --- BRANCH MANAGEMENT ---
def create_branch(repo, source, branch):
    response = requests.post(
        f"{BASE_URL}/api/v1/repositories/{repo}/branches",
        auth=AUTH,
        json={"source": source, "name": branch}
    )
    if response.status_code == 409:
        return branch
    response.raise_for_status()
    # return the branch name for simple downstream usage
    return branch

def commit(repo, branch, message):
    response = requests.post(
        f"{BASE_URL}/api/v1/repositories/{repo}/commits",
        auth=AUTH,
        json={"branch": branch, "message": message}
    )
    response.raise_for_status()
    return response.json()

def get_branches(repo):
    response = requests.get(
        f"{BASE_URL}/api/v1/repositories/{repo}/branches",
        auth=AUTH,
    )
    response.raise_for_status()
    return response.json()

def delete_branch(repo, branch):
    response = requests.delete(
        f"{BASE_URL}/api/v1/repositories/{repo}/branches/{branch}",
        auth=AUTH,
    )
    response.raise_for_status()
    return response.json()

def push(repo, branch, message):
    # Commit changes to the branch
    commit_response = commit(repo, branch, message)
    return commit_response

# --- OBJECT MANAGEMENT ---
def get_objects(repo, branch):
    # list objects under a ref/branch
    response = requests.get(
        f"{BASE_URL}/api/v1/repositories/{repo}/refs/{branch}/objects",
        auth=AUTH,
    )
    response.raise_for_status()
    return response.json()


def download_object(repo, branch, object_path, destination):
    """Download an object from lakeFS (by branch/ref + path) and write to `destination`.

    destination: local filesystem path where bytes will be written.
    """
    response = requests.get(
        f"{BASE_URL}/api/v1/repositories/{repo}/refs/{branch}/objects",
        auth=AUTH,
        params={"path": object_path},
        stream=True,
    )
    response.raise_for_status()

    dest_path = Path(destination)
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    # Stream-write to avoid large memory usage
    with open(dest_path, "wb") as fh:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                fh.write(chunk)

    return str(dest_path)


def upload_object(repo, branch, object_path, file_content):
    # simple PUT to objects endpoint using branch as ref and path as query param
    response = requests.put(
        f"{BASE_URL}/api/v1/repositories/{repo}/objects",
        auth=AUTH,
        params={"branch": branch, "path": object_path},
        data=file_content,
    )
    response.raise_for_status()
    return response.json()

def delete_object(repo, branch, object_path):
    response = requests.delete(
        f"{BASE_URL}/api/v1/repositories/{repo}/objects",
        auth=AUTH,
        params={"branch": branch, "path": object_path},
    )
    response.raise_for_status()
    return response.json()