from lakefs_client import get_object, download_object, upload_object, delete_object

def test_get_object():
    obj = get_object("lakefs-tutorial", "main", "sb_dataset_v3.csv")
    assert obj["path"] == "sb_dataset_v3.csv"
    assert obj["size"] > 0

def test_download_object():
    local_path = download_object("lakefs-tutorial", "main", "sb_dataset_v3.csv", "data/raw/sb_dataset_v3.csv")
    assert os.path.exists(local_path)
    assert os.path.getsize(local_path) > 0

def test_upload_and_delete_object():
    # Upload a test object
    content = "test content"
    upload_response = upload_object("lakefs-tutorial", "main", "test_object.txt", content)
    assert upload_response["path"] == "test_object.txt"

    # Verify the object exists
    obj = get_object("lakefs-tutorial", "main", "test_object.txt")
    assert obj["path"] == "test_object.txt"
    assert obj["size"] == len(content)

    # Delete the object
    delete_response = delete_object("lakefs-tutorial", "main", "test_object.txt")
    assert delete_response is None  # Assuming delete returns no content

    # Verify the object is deleted
    try:
        get_object("lakefs-tutorial", "main", "test_object.txt")
        assert False, "Object should have been deleted"
    except Exception as e:
        assert "not found" in str(e).lower()

def test_commit():
    commit_response = commit("lakefs-tutorial", "main", "Test commit message")
    assert "commit_id" in commit_response