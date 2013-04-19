.PHONY: test

all: test

env:
	@virtualenv env

deps: requirements.txt env
	@env/bin/pip install -r requirements.txt

test: deps
	@env/bin/python boto_test.py

clean:
	@rm -rf env
