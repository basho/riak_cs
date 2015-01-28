.PHONY: test clean all
.DEFAULT: all

DEPS = env/lib/python2.7/site-packages
BIN = env/bin

## overwrite auth.py because boto has bugs of auth v4
## diff from 2.35.1: bb5e02b1c61b3cf03ba9cafc7a3f6c56adcf92ad.patch
PATCHED_AUTH_PY = auth.py.patched

all: test

env:
	@virtualenv env

$(DEPS) $(BIN): requirements.txt env
	@env/bin/pip install -r requirements.txt
	cp $(PATCHED_AUTH_PY) $(DEPS)/boto/auth.py
	rm -f $(DEPS)/boto/auth.pyc

test: $(DEPS) $(BIN)
	@env/bin/python boto_test.py

clean:
	@rm -rf env
