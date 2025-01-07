# How to run Examples

> [!IMPORTANT]  
> Our examples are not designed to be used with compute outside of the docker network (e.g. external Spark). For production deployments external object storage is required. Please check our [docs](http://docs.lakekeeper.io) for more information.

All examples are self-contained and run all services required for the specific scenarios inside of docker compose. To run each scenario, you need to have `docker` installed on your machine. Most docker distributes come with built-in `docker compose` today.

After starting the examples, please wait for a Minute after all images are pulled - especially keycloak takes some time to start and setup. Please check the `README.md`s in each examples folder for more information on this specific example.

## Development / Re-Build image
Running `docker compose up` for each example starts the `latest-main` release of Lakekeeper. To build a fresh image use:

```bash
docker compose -f docker-compose.yaml -f docker-compose-build.yaml up --build
```

