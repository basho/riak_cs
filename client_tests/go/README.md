# Riak CS Go Tests

## Dependencies

- Go Lang runtime installation, see http://golang.org/doc/ .

## Configuration

Before execution, settings below should be done for Riak CS:

- Create an user and export its key and secret as environment
  variables.
- Create an bucket owned by the user, export its name as an an
  environment variable.
- Export proxy information to connect Riak CS.

Sample bash script to export these settings:

```
export AWS_ACCESS_KEY_ID=8P4GB-NTTTWKDBP6AVLF
export AWS_SECRET_ACCESS_KEY=1xJYjxqtVzogYmo697ZzNOVp8r0dMvWbnPVfiQ==
export CS_HTTP_PORT=15018
export CS_BUCKET=test-gof3r
export CS_KEY=complete-multipart-upload
```

`CS_HTTP_PORT` can be ommitted if `HTTP_PROXY` is set
(e.g. `HTTP_PROXY=127.0.0.1:15018`).
`CS_BUCKET` and `CS_KEY` are optional, the default values are `test`
and an epoch seconds each.

## Execution

Installation of s3gof3r and gof3r command line is automatically done
by `Makefile`.  So all you should do is just `make`.

```
cd client_tests/go
make
```

This executes one test target `test-mp-put-get`.

- First creates input file of 50MB at `./tmp`,
- then executes multipart upload with 5MB part size,
- finally gets the object and compares it with the input file.
