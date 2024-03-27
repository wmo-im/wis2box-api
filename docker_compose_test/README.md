# docker compose test

This directory contains the files to test the wis2box-api within a docker-compose stack.

test.env contains the environment variables for the test.


## Start the test environment

Run the docker-compose stack in the background:

```bash
docker-compose up -d --build
```


## Run the tests

pytest -s ./tests/integration

## Debug

Check the logs of the wis2box-api container:

```bash
docker logs wis2box-api-test
```