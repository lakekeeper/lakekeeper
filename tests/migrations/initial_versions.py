import tomllib

def get_crate_version_parts():
    with open("../../crates/lakekeeper/Cargo.toml", "rb") as f:
        cargo_data = tomllib.load(f)
    version_str = cargo_data["package"]["version"]
    version_parts = [ int(x) for x in version_str.split(".") ]
    if len(version_parts) < 2:
        raise ValueError(f"Minor version not specified in {version_str}")
    return version_parts

def previous_minor_version_of(major, minor):
    if minor - 1 >= 0:
        # E.g. when on 0.9.2 the latest release of 0.8 is tagged on quay.io as `v0.8`
        return f"{major}.{minor - 1}"
    else:
        # The major version was bumped recently.
        # Assume the latest release of the previous major version is tagged on quay as follows.
        return f"{major - 1}"

if __name__ == "__main__":
    crate_version_parts  = get_crate_version_parts()
    prev_minor = previous_minor_version_of(crate_version_parts[0], crate_version_parts[1])
    # Referencing tags for quay.io/lakekeeper/catalog
    versions = [
        "latest", # the latest version released to quay.io
        f"v{prev_minor}"
    ]
    # Print in the format expected by the migrations workflow.
    print(f"initial-versions={versions}")

