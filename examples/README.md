# How to run Examples

> [!IMPORTANT]  
> Our examples are not designed to be used with compute outside of the docker network (e.g. external Spark). For production deployments external object storage is required. Please check our [docs](http://docs.lakekeeper.io) for more information

Multiple examples are provided in the `examples` directory. All examples are self-contained and run all services required for the specific scenarios inside of docker compose. To run each scenario, you need to have `docker` installed on your machine. Most docker distributes come with built-in `docker compose` today.

To run each example run the following commands (example for `minimal` example):

```bash
cd examples/minimal
docker compose up
```

Most examples come with a jupyter notebook that can now be accessed at `http://localhost:8888` in your browser. You can also access the UI of Lakekeeper at `http://localhost:8181`.

Running `docker compose up` starts the latest release of Lakekeeper. To build a new image based on the latest code, run:

```bash
docker compose -f docker-compose.yaml -f docker-compose-build.yaml up --build
```

Once jupyter is started, you can browse and run the available notebooks.