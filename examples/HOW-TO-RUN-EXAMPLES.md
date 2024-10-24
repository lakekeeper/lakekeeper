# How to run Examples

Multiple examples are provided in the `examples` directory.
Each example uses docker-compose to run the services required for the specific scenarios.
To run each scenario, you need to have `docker` installed on your machine. Most docker distributes come with built-in
`docker compose` today.

To run each example run the following commands (example for `self-contained` example):

```bash
cd examples/self-contained
docker compose up
```

Most examples come with a jupyter notebook that can now be accessed at `http://localhost:8888` in your browser.

Running `docker compose up` starts the latest release of Lakekeeper. To build a new image based on the latest code, run:

```bash
docker compose -f docker-compose.yaml -f docker-compose-build.yaml up --build
```

Once jupyter is started, you can browse and run the available notebooks.