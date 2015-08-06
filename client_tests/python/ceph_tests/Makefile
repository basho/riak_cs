.PHONY: test clean env all
.DEFAULT: all

DEPS = env/lib/python2.7/site-packages
BIN = env/bin
NOSE_ARGS = s3tests.functional.test_s3 -A
NOSE_ATTR = (method != "post")
NOSE_ATTR += and (resource != "bucket.log")
NOSE_ATTR += and (not versioning)
NOSE_ATTR += and (not cors)
NOSE_ATTR += and (not fails_on_aws)
NOSE_ATTR += and (not fails_with_subdomain)
NOSE_ATTR += and (not fails_on_rcs)

all: test

s3-tests:
	@git clone --quiet https://github.com/basho/s3-tests \
		-b riakcs-2.1

env:
	@cd s3-tests && virtualenv env

$(DEPS) $(BIN): s3-tests env
	@cd s3-tests && env/bin/pip install -r requirements.txt

test: $(DEPS) $(BIN)
	@echo $(CS_HTTP_PORT)
	@./s3_conf.sh > s3-tests/s3test.conf
	cd s3-tests && \
	    S3TEST_CONF=s3test.conf env/bin/nosetests $(NOSE_ARGS) '$(NOSE_ATTR)'

clean:
	@rm -rf s3-tests
