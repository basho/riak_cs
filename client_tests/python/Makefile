.PHONY: test clean all
.DEFAULT: all

DEPS = env/lib/python2.7/site-packages
BIN = env/bin

all: test

env:
	@virtualenv env

$(DEPS) $(BIN): requirements.txt env
	@env/bin/pip install -r requirements.txt

test: $(DEPS) $(BIN)
	@env/bin/python boto_test.py

clean:
	@rm -rf env
