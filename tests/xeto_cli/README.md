## About Xeto CLI tests

Haxall's Xeto CLI enables data based on Xeto specs to be validated in more than one programming language.  Over time we expect there will be more than one Xeto CLI available for use.

The purpose of these tests are to verify Phable's compatability and ease of use with certain Xeto CLIs.

## Setup instructions

From the root of the `xeto_cli` directory perform the following:
 1. Create a directory called `temp_data` used for storing temporary input and stdout data used by the Xeto CLI
 2. With Docker Desktop running in the background, create a Docker image and container for the Xeto CLI and run the newly created container called `phable_haxall_cli_run` by executing the command:
 ```bash
 docker compose up
 ```

After these steps have been taken the enclosed tests should be able to pass.

Note: The Docker image created is based on the latest commit on [haxall](https://github.com/haxall/haxall).  Haxall continues to be developed and you might need to recreate a Docker container based on recent progress by following these steps again.

## Xeto specs

This test suite uses specs in `xeto_cli/xeto/phable`.  New specs can be added to this library.

## Getting involved

A great way to learn how Xeto works and gain confidence in the technology is to write tests to validate expected behavior for the Xeto CLI.  Also, simply understanding the written tests can be beneficial.

We welcome pull requests to Phable to add tests and improve existing ones.  Please note we will all need to work together to ensure the test suite minimizes duplicate tests, etc.
