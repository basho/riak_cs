.PHONY: all clean gof3r test-mp-put-get

CS_HTTP_PORT ?= 8080
HTTP_PROXY   ?= 127.0.0.1:$(CS_HTTP_PORT)
CS_BUCKET    ?= test
CS_KEY       ?= $(shell date '+%s')

TMPDIR       := $(PWD)/tmp
RANDFILE     := $(TMPDIR)/rand-50MB
GETFILE      := $(TMPDIR)/rand-get
GOOPTS       := --no-ssl

export GOPATH     := $(PWD)/env
export PATH       := $(GOPATH)/bin:$(PATH)
export HTTP_PROXY

all: test-mp-put-get

#### TESTS

## Test multipart upload completes without error
## See: https://github.com/basho/riak_cs/issues/855
test-mp-put-get: gof3r $(RANDFILE)
	gof3r put $(GOOPTS) -s 5242880 -p $(RANDFILE) -b $(CS_BUCKET) -k $(CS_KEY)
	rm -f $(GETFILE)
	gof3r get $(GOOPTS) -b $(CS_BUCKET) -k $(CS_KEY) - > $(GETFILE)
	diff $(RANDFILE) $(GETFILE)
	rm -f $(GETFILE)

### Environment setup

env:
	mkdir $(GOPATH)

gof3r: $(GOPATH)/bin/gof3r

$(GOPATH)/bin/gof3r: env
	go get github.com/rlmcpherson/s3gof3r/gof3r

$(RANDFILE):
	mkdir -p $(TMPDIR)
	dd if=/dev/urandom of=$(RANDFILE) bs=1M count=50

clean:
	rm -rf $(TMPDIR)
	rm -rf $(GOPATH)
