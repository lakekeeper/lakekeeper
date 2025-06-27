
# Integration tests

## OpenFGA

Some tests are run against an OpenFGA server.

```bash
# Start an OpenFGA server in a docker container
docker rm --force openfga-client && docker run -d --name openfga-client -p 36080:8080 -p 36081:8081 -p 36300:3000 openfga/openfga:v1.8 run

# Set Lakekeeper's OpenFGA endpoint
export LAKEKEEPER_TEST__OPENFGA__ENDPOINT="http://localhost:36081"

# Enable and run the tests
export TEST_OPENFGA=1
cargo nextest run --all-features --lib openfga
```
