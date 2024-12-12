set -e

export REMOTE="lakekeeper_docs"

# Ensures the presence of a specified remote repository for documentation.
# If the remote doesn't exist, it adds it using the provided URL.
# Then, it fetches updates from the remote repository.
create_or_update_docs_remote () {
    echo " --> create or update docs remote"
    
    # Check if the remote exists before attempting to add it
    git config "remote.${REMOTE}.url" >/dev/null ||
    git remote add "${REMOTE}" https://github.com/lakekeeper/lakekeeper.git
    
    # Fetch updates from the remote repository
    git fetch "${REMOTE}"
}

# Pulls updates from a specified branch of a remote repository.
# Arguments:
#   $1: Branch name to pull updates from
pull_remote () {
    echo " --> pull remote"
    
    local BRANCH="$1"
    
    assert_not_empty "${BRANCH}"
    
    # Perform a pull from the specified branch of the remote repository
    git pull "${REMOTE}" "${BRANCH}"
}

# Pushes changes from a local branch to a specified branch of a remote repository.
# Arguments:
#   $1: Branch name to push changes to
push_remote () {
    echo " --> push remote"
    
    local BRANCH="$1"
    
    assert_not_empty "${BRANCH}"
    
    # Push changes to the specified branch of the remote repository
    git push "${REMOTE}" "${BRANCH}"
}

# Installs or upgrades dependencies specified in the 'requirements.txt' file using pip.

install_deps () {
    echo " --> install deps"
    
    # Use pip to install or upgrade dependencies from the 'requirements.txt' file quietly
    pip -q install -r requirements.txt --upgrade
}

# Checks if a provided argument is not empty. If empty, displays an error message and exits with a status code 1.
# Arguments:
#   $1: Argument to check for emptiness
assert_not_empty () {
    if [ -z "$1" ]; then
        echo "No argument supplied"
        
        # Exit with an error code if no argument is provided
        exit 1
    fi
}

# Updates version information within the mkdocs.yml file for a specified LAKEKEEPER_VERSION.
# Arguments:
#   $1: LAKEKEEPER_VERSION - The version number used for updating the mkdocs.yml file.
update_version () {
    echo " --> update version"
    
    local LAKEKEEPER_VERSION="$1"
    
    assert_not_empty "${LAKEKEEPER_VERSION}"
    
    # Update version information within the mkdocs.yml file using sed commands
    if [ "$(uname)" == "Darwin" ]
    then
        sed -i '' -E "s/(^site\_name:[[:space:]]+docs\/).*$/\1${LAKEKEEPER_VERSION}/" ${LAKEKEEPER_VERSION}/mkdocs.yml
    elif [ "$(expr substr $(uname -s) 1 5)" == "Linux" ]
    then
        sed -i'' -E "s/(^site_name:[[:space:]]+docs\/)[^[:space:]]+/\1${LAKEKEEPER_VERSION}/" "${LAKEKEEPER_VERSION}/mkdocs.yml"
    fi
    
}

create_nightly () {
    echo " --> create nightly"
    
    # Ensure the docs/docs directory exists
    mkdir -p docs/docs/
    
    # Remove any existing 'nightly' directory and recreate it
    rm -rf docs/docs/nightly/
    mkdir docs/docs/nightly/
    
    # Create symbolic links and copy configuration files for the 'nightly' documentation
    ln -s "../../../../docs/docs/" docs/docs/nightly/docs
    cp "../docs/mkdocs.yml" docs/docs/nightly/
    
    cd docs/docs/
    
    # Update mkdocs version field within the 'nightly' documentation
    update_version "nightly"
    cd -
}

# Finds and retrieves the latest version of the documentation based on the directory structure.
# Assumes the documentation versions are numeric folders within 'docs/docs/'.
get_latest_version () {
  # Find the latest numeric folder within 'docs/docs/' structure
  local latest=$(ls -d docs/docs/[0-9]* | sort -V | tail -1)

  # Extract the version number from the latest directory path
  local latest_version=$(basename "${latest}")
  
  # Return the latest version number
  echo "${latest_version}"
}

# Creates a 'latest' version of the documentation based on a specified LAKEKEEPER_VERSION.
# Arguments:
#   $1: LAKEKEEPER_VERSION - The version number of the documentation to be treated as the latest.
create_latest () {
  echo " --> create latest"

  local LAKEKEEPER_VERSION="$1"

  assert_not_empty "${LAKEKEEPER_VERSION}"


  # Remove any existing 'latest' directory and recreate it
  rm -rf docs/docs/latest/
  mkdir docs/docs/latest/

  # Create symbolic links and copy configuration files for the 'latest' documentation
  ln -s "../${LAKEKEEPER_VERSION}/docs" docs/docs/latest/docs
  cp "docs/docs/${LAKEKEEPER_VERSION}/mkdocs.yml" docs/docs/latest/

  cd docs/docs/

  # Update version information within the 'latest' documentation
  update_version "latest"
  cd -
}

# Sets up local worktrees for the documentation and performs operations related to different versions.
pull_versioned_docs () {
  echo " --> pull versioned docs"

  # Ensure the remote repository for documentation exists and is up-to-date
  create_or_update_docs_remote

  # Add local worktrees for documentation and javadoc either from the remote repository
  # or from a local branch.
  local docs_branch="${LAKEKEEPER_VERSIONED_DOCS_BRANCH:-${REMOTE}/docs}"
  git worktree add -f docs/docs "${docs_branch}"

  # Retrieve the latest version of documentation for processing
  local latest_version=$(get_latest_version)

  # Create the 'latest' version of documentation
  create_latest "${latest_version}"

  # Create the 'nightly' version of documentation
  create_nightly
}

clean () {
  echo " --> clean"

  # Temporarily disable script exit on errors to ensure cleanup continues
  set +e

  # Remove temp directories and related Git worktrees
  rm -rf docs/docs/latest &> /dev/null
  rm -rf docs/docs/nightly &> /dev/null

  git worktree remove docs/docs &> /dev/null

  # Remove any remaining artifacts
  rm -rf site/

  set -e # Re-enable script exit on errors
}
