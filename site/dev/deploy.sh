set -e

./dev/setup_env.sh

mkdocs gh-deploy --no-history 
