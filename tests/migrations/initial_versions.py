import json
import re
import subprocess

RELEASE_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")


def last_release_in_history():
    """The highest release tag (vX.Y.Z) in the history of HEAD.

    On `main` this is the last release cut from `main`, on a `rel-X-Y` branch
    the last release of that line. A release pull request has the history of
    its base branch, so it gets the same answer as the branch it releases.
    """
    tags = subprocess.run(
        ["git", "tag", "--merged", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.split()
    releases = [
        tuple(int(part) for part in match.groups())
        for match in map(RELEASE_TAG.match, tags)
        if match
    ]
    if not releases:
        raise ValueError("No release tag (vX.Y.Z) in the history of HEAD. Fetch the tags.")
    return max(releases)


def previous_minor_version_of(major, minor):
    if minor - 1 >= 0:
        # E.g. when on 0.9.2 the latest release of 0.8 is tagged on quay.io as `v0.8`
        return f"{major}.{minor - 1}"
    else:
        # The major version was bumped recently.
        # Assume the latest release of the previous major version is tagged on quay as follows.
        return f"{major - 1}"


if __name__ == "__main__":
    major, minor, _ = last_release_in_history()
    # Referencing tags for quay.io/lakekeeper/catalog. Each `vX.Y` tag points to the
    # highest patch of that release line. The test upgrades from the releases users
    # come from: the newest patch of this line and of the minor before it.
    versions = [
        f"v{major}.{minor}",
        f"v{previous_minor_version_of(major, minor)}",
    ]
    # Print in the format expected by the migrations workflow.
    print(f"initial-versions={json.dumps(versions)}")
