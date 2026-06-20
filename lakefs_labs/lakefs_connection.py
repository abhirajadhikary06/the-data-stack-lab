import requests
from dotenv import load_dotenv
import os
load_dotenv()

ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
BASE_URL = "http://localhost:8000"
repo = "lakefs-tutorial"

# Authenticate Requests with the access key and secret key
response = requests.get(
    "http://localhost:8000/api/v1/repositories",
    auth=(ACCESS_KEY, SECRET_KEY)
)
print(response.json())

# List Repositories
response = requests.get(
    f"{BASE_URL}/api/v1/repositories",
    auth=(ACCESS_KEY, SECRET_KEY)
)

# List Branches
response = requests.get(
    f"{BASE_URL}/api/v1/repositories/{repo}/branches",
    auth=(ACCESS_KEY, SECRET_KEY)
)

# Create Branches
response = requests.post(
    f"{BASE_URL}/api/v1/repositories/{repo}/branches",
    auth=(ACCESS_KEY, SECRET_KEY),
    json={"name": "feature-polars", "source": "main"}
)

# Delete a branch
branch_name = "feature-polars"
response = requests.delete(
    f"{BASE_URL}/api/v1/repositories/{repo}/branches/{branch_name}",
    auth=(ACCESS_KEY, SECRET_KEY)
)

# List Branches after Creating
response = requests.get(
    f"{BASE_URL}/api/v1/repositories/{repo}/branches",
    auth=(ACCESS_KEY, SECRET_KEY)
)

if response.status_code == 200:
    data = response.json()
    # Extract just the branch IDs/names from the results list
    branches = [branch['id'] for branch in data.get('results', [])]
    
    print("Available branches:")
    for branch in branches:
        print(f"- {branch}")
else:
    print(f"Error: {response.status_code} - {response.text}")