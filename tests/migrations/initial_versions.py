import tomllib

def get_versions_to_test():
    with open("../../crates/lakekeeper/Cargo.toml", "rb") as f:
        cargo_data = tomllib.load(f)
    return cargo_data["package"]["metadata"]["migration-tests"]["versions"]

if __name__ == "__main__":
    versions = get_versions_to_test()
    print(f"initial-versions={versions}")

