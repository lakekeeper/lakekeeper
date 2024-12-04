#!/usr/bin/env bash

set -e

./dev/setup_env.sh

mkdocs serve --dirty -a localhost:8001 --watch .
