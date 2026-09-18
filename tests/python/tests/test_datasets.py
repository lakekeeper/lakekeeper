"""End-to-end dataset tests against a running Lakekeeper and real object storage.

Driven straight over the REST API with `requests`, the way the Lance suite drives
the Generic Table API. The Python client lives in `lakekeeper-clients`, so there
is nothing in this repo for these to import.

Import is the reason these need real storage. It registers what it finds by
*listing the prefix*, so proving it works means putting objects there and asking
the server to discover them -- something no in-memory backend can stand in for.
Objects are written with the warehouse's own credentials (`io_fsspec`): the
fixture is test scaffolding, not the path under test. Credential vending has a
test of its own.
"""

import time
import uuid
from urllib.parse import quote

import conftest
import pytest
import requests

#: Prefix the server uses to carry the branch head in a 409's error stack.
#: `ErrorModel` has no structured payload slot, so this is the agreed form.
HEAD_PREFIX = "current-snapshot-id: "


class DatasetApi:
    """The dataset routes of one warehouse, as thin as the tests allow."""

    def __init__(self, warehouse: conftest.Warehouse, namespace: conftest.Namespace):
        # The catalog URL ends in /catalog/v1/; datasets live beside it under
        # /lakekeeper/v1/.
        root = warehouse.server.catalog_url.split("/catalog/")[0]
        self.base = (
            f"{root}/lakekeeper/v1/{warehouse.warehouse_id}"
            f"/namespaces/{namespace.url_name}/datasets"
        )
        self.headers = {"Authorization": f"Bearer {warehouse.access_token}"}

    def request(self, method: str, path: str, **kwargs) -> requests.Response:
        headers = {**self.headers, **kwargs.pop("headers", {})}
        return requests.request(
            method, f"{self.base}{path}", headers=headers, **kwargs
        )

    def json(self, method: str, path: str, **kwargs) -> dict:
        response = self.request(method, path, **kwargs)
        assert response.ok, f"{method} {path} -> {response.status_code}: {response.text}"
        return response.json() if response.content else {}


class Dataset:
    """A dataset under test."""

    def __init__(self, api: DatasetApi, name: str):
        self.api = api
        self.name = name
        self.path = f"/{quote(name, safe='')}"

    def load(self) -> dict:
        return self.api.json("GET", self.path)

    @property
    def location(self) -> str:
        return self.load()["dataset"]["location"]

    def commit(
        self,
        added=(),
        removed=(),
        branch: str = "main",
        parent_snapshot_id=None,
    ) -> requests.Response:
        body = {"added": list(added), "removed": list(removed)}
        if parent_snapshot_id is not None:
            body["parent-snapshot-id"] = parent_snapshot_id
        return self.api.request(
            "POST", f"{self.path}/branches/{quote(branch, safe='')}/commits", json=body
        )

    def commit_ok(self, **kwargs) -> dict:
        response = self.commit(**kwargs)
        assert response.ok, f"commit -> {response.status_code}: {response.text}"
        return response.json()

    def keys(self, ref: str = "main") -> list:
        """Every logical key the ref resolves to.

        A page may come back **short or empty while files remain** -- the server
        bounds each scan by key range and applies removals afterwards -- so only
        the absence of a token ends the walk.
        """
        found, token = [], None
        while True:
            params = {"pageToken": token} if token else None
            page = self.api.json(
                "GET", f"{self.path}/refs/{quote(ref, safe='')}/files", params=params
            )
            found.extend(page.get("files", []))
            token = page.get("next-page-token")
            if not token:
                return found

    def tag(self, name: str, ref: str = "main") -> dict:
        return self.api.json(
            "POST",
            f"{self.path}/refs",
            json={"name": name, "typ": "tag", "source": {"type": "ref", "name": ref}},
        )

    def branch(self, name: str, snapshot_id: str) -> dict:
        return self.api.json(
            "POST",
            f"{self.path}/refs",
            json={
                "name": name,
                "typ": "branch",
                "source": {"type": "snapshot", "snapshot-id": snapshot_id},
            },
        )

    def import_objects(self, **body) -> requests.Response:
        return self.api.request("POST", f"{self.path}/import", json=body)

    def import_ok(self, **body) -> dict:
        response = self.import_objects(**body)
        assert response.ok, f"import -> {response.status_code}: {response.text}"
        return response.json()

    def credentials(self) -> list:
        payload = self.api.json(
            "GET",
            f"{self.path}/credentials",
            headers={"x-iceberg-access-delegation": "vended-credentials"},
        )
        return payload.get("storage-credentials", [])


@pytest.fixture
def dataset(warehouse: conftest.Warehouse, namespace: conftest.Namespace) -> Dataset:
    api = DatasetApi(warehouse, namespace)
    name = f"ds-{uuid.uuid4().hex[:8]}"
    api.json("POST", "", json={"name": name})
    return Dataset(api, name)


def sorted_keys(files) -> list:
    return sorted(f["logical-key"] for f in files)


def entry(files, key: str) -> dict:
    return next(f for f in files if f["logical-key"] == key)


def _file(key: str, location: str) -> dict:
    return {"logical-key": key, "physical-path": f"{location}/{key}"}


def put(io_fsspec, location: str, key: str, body: bytes = b"x" * 16) -> None:
    with io_fsspec.open(f"{location}/{key}", "wb") as handle:
        handle.write(body)


def test_commit_tag_and_read_back(dataset):
    location = dataset.location

    first = dataset.commit_ok(
        added=[_file("a.parquet", location), _file("b.parquet", location)]
    )
    dataset.tag("v1")

    second = dataset.commit_ok(
        added=[_file("c.parquet", location)],
        removed=["a.parquet"],
        parent_snapshot_id=first["snapshot-id"],
    )
    assert second["parent-snapshot-id"] == first["snapshot-id"]

    # The tag keeps naming what it named, however far main moves on. This is the
    # whole reproducibility claim, so it is asserted against the server rather
    # than trusted.
    assert sorted_keys(dataset.keys("v1")) == ["a.parquet", "b.parquet"]
    assert sorted_keys(dataset.keys("main")) == ["b.parquet", "c.parquet"]


def test_stale_commit_conflicts_and_reports_the_head(dataset):
    location = dataset.location
    first = dataset.commit_ok(added=[_file("a.parquet", location)])
    dataset.commit_ok(
        added=[_file("b.parquet", location)], parent_snapshot_id=first["snapshot-id"]
    )

    # Still believes the branch is on the first snapshot.
    response = dataset.commit(
        added=[_file("c.parquet", location)], parent_snapshot_id=first["snapshot-id"]
    )
    assert response.status_code == 409, response.text

    # The conflict has to carry the head, or a writer cannot rebase without a
    # separate round trip.
    stack = response.json()["error"].get("stack") or []
    head = next(e[len(HEAD_PREFIX) :].strip() for e in stack if e.startswith(HEAD_PREFIX))
    assert head != first["snapshot-id"]


def test_branch_diverges_without_touching_main(dataset):
    location = dataset.location
    base = dataset.commit_ok(added=[_file("shared.parquet", location)])

    dataset.branch("experiment", snapshot_id=base["snapshot-id"])
    dataset.commit_ok(
        added=[_file("only-on-experiment.parquet", location)],
        branch="experiment",
        parent_snapshot_id=base["snapshot-id"],
    )

    assert sorted_keys(dataset.keys("main")) == ["shared.parquet"]
    assert sorted_keys(dataset.keys("experiment")) == [
        "only-on-experiment.parquet",
        "shared.parquet",
    ]


def test_import_registers_objects_already_in_storage(dataset, io_fsspec):
    """The M2 path: put objects in the bucket, let the server discover them.

    Nothing is copied -- import writes manifest rows for what it lists, so the
    physical paths it records are the objects that were already there.
    """
    location = dataset.location
    keys = ["shard=00/a.parquet", "shard=00/b.parquet", "shard=01/c.parquet", "notes.txt"]
    for key in keys:
        put(io_fsspec, location, key)

    result = dataset.import_ok()
    assert result["imported"] == len(keys), result
    assert result["truncated"] is False

    files = dataset.keys("main")
    assert sorted_keys(files) == sorted(keys)
    # Sizes come from the listing, and content type is inferred from the
    # extension -- neither was supplied by the caller.
    assert entry(files, "notes.txt")["size"] == 16
    assert (
        entry(files, "shard=00/a.parquet")["content-type"]
        == "application/vnd.apache.parquet"
    )
    assert entry(files, "notes.txt")["content-type"] == "text/plain"
    # The physical path must be the object that was already there, not a copy.
    assert entry(files, "notes.txt")["physical-path"].endswith("notes.txt")


def test_import_honours_suffix_and_sub_prefix(dataset, io_fsspec):
    location = dataset.location
    for key in ["shard=00/a.parquet", "shard=00/b.txt", "shard=01/c.parquet"]:
        put(io_fsspec, location, key)

    result = dataset.import_ok(**{"sub-prefix": "shard=00", "suffix": ".parquet"})
    assert result["imported"] == 1
    assert sorted_keys(dataset.keys("main")) == ["shard=00/a.parquet"]


def test_import_refuses_a_sub_prefix_that_escapes_the_dataset(dataset):
    response = dataset.import_objects(**{"sub-prefix": "../elsewhere"})
    assert response.status_code == 400, response.text


def test_vended_credentials_read_the_dataset_back(dataset, io_fsspec):
    """The delegation path: read a file with what the server hands out.

    A profile without STS vends nothing -- Iceberg tables fall back to remote
    signing there, and datasets have no signing route -- so this skips rather
    than asserting a capability the warehouse does not have.
    """
    import fsspec

    location = dataset.location
    put(io_fsspec, location, "train/0001.txt", b"train/0001.txt")
    dataset.import_ok()
    dataset.tag("v1")

    credentials = dataset.credentials()
    if not credentials:
        pytest.skip("warehouse storage profile vends no credentials")

    config = credentials[0]["config"]
    if "s3.access-key-id" not in config:
        pytest.skip("vended credentials are mapped for S3 only")

    client_kwargs = {"region_name": config["s3.region"], "use_ssl": False}
    if "s3.endpoint" in config:
        client_kwargs["endpoint_url"] = config["s3.endpoint"]
    fs = fsspec.filesystem(
        "s3",
        anon=False,
        key=config["s3.access-key-id"],
        secret=config["s3.secret-access-key"],
        token=config.get("s3.session-token"),
        client_kwargs=client_kwargs,
    )

    physical = entry(dataset.keys("v1"), "train/0001.txt")["physical-path"]
    with fs.open(physical, "rb") as handle:
        assert handle.read() == b"train/0001.txt"


def test_rescan_sync_detects_modifications_and_deletions(dataset, io_fsspec):
    """The M2 re-scan: make the snapshot mirror the bucket, not just grow."""
    location = dataset.location
    for key in ["keep.txt", "changed.txt", "gone.txt"]:
        put(io_fsspec, location, key, b"original")

    assert dataset.import_ok()["imported"] == 3

    # Re-importing an untouched prefix must be a no-op, or a nightly sync would
    # rewrite the whole manifest every night.
    noop = dataset.import_ok(mode="sync")
    assert (noop["imported"], noop["modified"], noop["removed"]) == (0, 0, 0)

    put(io_fsspec, location, "changed.txt", b"a different length entirely")
    io_fsspec.rm(f"{location}/gone.txt")
    put(io_fsspec, location, "brand-new.txt", b"new")

    result = dataset.import_ok(mode="sync")
    assert (result["imported"], result["modified"], result["removed"]) == (1, 1, 1)
    assert sorted_keys(dataset.keys("main")) == [
        "brand-new.txt",
        "changed.txt",
        "keep.txt",
    ]


def test_add_only_mode_never_removes(dataset, io_fsspec):
    """The default must not drop entries just because an object vanished."""
    location = dataset.location
    for key in ["a.txt", "b.txt"]:
        put(io_fsspec, location, key)
    dataset.import_ok()

    io_fsspec.rm(f"{location}/b.txt")
    assert dataset.import_ok()["removed"] == 0
    assert sorted_keys(dataset.keys("main")) == ["a.txt", "b.txt"]


def test_sync_refuses_a_truncated_scan(dataset, io_fsspec):
    """Absence is only evidence of deletion if the whole prefix was seen."""
    location = dataset.location
    for key in ["a.txt", "b.txt", "c.txt"]:
        put(io_fsspec, location, key)

    response = dataset.import_objects(mode="sync", **{"max-files": 2})
    assert response.status_code == 400, response.text


def test_sync_on_a_fresh_dataset_is_the_first_import(dataset, io_fsspec):
    """Sync must be usable as the first thing done to a dataset.

    A branch with no commits resolves to no files; reading that as an error would
    make sync unusable for the case it is most obviously wanted for.
    """
    location = dataset.location
    for key in ["a.txt", "b.txt"]:
        put(io_fsspec, location, key)

    result = dataset.import_ok(mode="sync")
    assert (result["imported"], result["modified"], result["removed"]) == (2, 0, 0)
    assert sorted_keys(dataset.keys("main")) == ["a.txt", "b.txt"]


def test_queued_import_runs_on_the_task_queue(dataset, io_fsspec):
    """The scan moves off the request path; the snapshot appears when it lands."""
    location = dataset.location
    for key in ["q/a.txt", "q/b.txt", "q/c.txt"]:
        put(io_fsspec, location, key)

    # Both ids are omitted rather than null when unset, so read them with `get`.
    result = dataset.import_ok(queued=True)
    assert result.get("task-id") is not None
    assert result.get("snapshot-id") is None

    deadline = time.time() + 60
    while time.time() < deadline:
        keys = sorted_keys(dataset.keys("main"))
        if keys:
            assert keys == ["q/a.txt", "q/b.txt", "q/c.txt"]
            return
        time.sleep(1)
    raise AssertionError("queued import did not land within 60s")
