.PHONY: test clean env all
.DEFAULT: all

DEPS = env/lib/python2.7/site-packages
BIN = env/bin

all: test

s3-tests:
	@git clone --quiet https://github.com/basho/s3-tests

env:
	@cd s3-tests && virtualenv env

$(DEPS) $(BIN): s3-tests env
	@cd s3-tests && env/bin/pip install -r requirements.txt

test: $(DEPS) $(BIN)
	@echo $(CS_HTTP_PORT)
	@./s3_conf.sh &> s3-tests/s3test.conf
	@cd s3-tests && S3TEST_CONF=s3test.conf env/bin/nosetests s3tests.functional.test_s3

clean:
	@rm -rf s3-tests
