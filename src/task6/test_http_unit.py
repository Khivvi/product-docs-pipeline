from pipeline.http import fetch_url, sha256


def test_sha256_deterministic():
    assert sha256(b"abc") == sha256(b"abc")


def test_sha256_changes():
    assert sha256(b"abc") != sha256(b"abcd")
