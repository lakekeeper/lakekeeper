#!/usr/bin/env bash

set -e

./dev/setup_env.sh

mkdocs build
