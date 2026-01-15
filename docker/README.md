## About Xeto CLI

Haxall's Xeto CLI enables data based on Xeto specs to be validated in more than one programming language.  Over time we expect there will be more than one Xeto CLI available for use.

Below are instructions on how to create a Docker container running the Haxall Xeto CLI which may be used for testing or experimenting with Xeto.

Note: The container is only intended for use with the Xeto CLI — the Haxall web server is not started, but support for it could be added in the future.

## Directory structure

The Docker setup requires the `phable` repo and the [xeto](https://github.com/Project-Haystack/xeto) repo to be siblings under a shared parent directory:

```
(parent dir)/
├── phable/     ← this repo
└── xeto/       ← Project-Haystack/xeto repo
```

Clone the xeto repo alongside `phable` if you haven't already:

```bash
git clone https://github.com/Project-Haystack/xeto
```

The `xeto/` directory is copied into the Docker image at build time and also mounted as a volume at runtime, so local changes to xeto specs are reflected without rebuilding the image.

Note:  Keeping a local xeto repo also makes it easy to draft and test changes before proposing them upstream to [Project-Haystack/xeto](https://github.com/Project-Haystack/xeto).

## Setup instructions

From the root of the `phable` directory, with Docker Desktop running in the background, create a Docker image and container for the Xeto CLI and run the newly created container called `phable_haxall_cli_run` by executing the command:

```bash
docker compose -f docker/docker-compose.yml up
```

After these steps have been taken all tests in Phable involving the Xeto CLI should be able to pass.

Note: The Docker image is built from the latest commit on [haxall](https://github.com/haxall/haxall).  If haxall has been updated and you need to incorporate those changes, rebuild the image without cache and restart the container:

```bash
docker compose -f docker/docker-compose.yml build --no-cache && docker compose -f docker/docker-compose.yml up
```

## Xeto specs

Phable's test suite uses specs defined in `docker/src/xeto/phable.test`.  New specs can be added to this library.  Also, other Xeto libraries may be added to the `docker/src/xeto` directory.

## Getting involved

A great way to learn how Xeto works and gain confidence in the technology is to write tests to validate expected behavior for the Xeto CLI.  Also, simply understanding the written tests can be beneficial.

We welcome pull requests to Phable to add tests and improve existing ones.  Please note we will all need to work together to ensure the test suite minimizes duplicate tests, etc.
