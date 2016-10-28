.PHONY: test test-auth-v2 test-auth-v4 clean all
.DEFAULT: all

DEPS = env/lib/python2.7/site-packages
BIN = env/bin

## overwrite auth.py because boto has bugs of auth v4
## diff from 2.35.1: bb5e02b1c61b3cf03ba9cafc7a3f6c56adcf92ad.patch
PATCHED_AUTH_PY = auth.py.patched

all: test

env:
	@virtualenv2 env

$(DEPS) $(BIN): requirements.txt env
	@env/bin/pip install -r requirements.txt
	cp $(PATCHED_AUTH_PY) $(DEPS)/boto/auth.py
	rm -f $(DEPS)/boto/auth.pyc

test: test-auth-v2 test-auth-v4

test-auth-v2: $(DEPS) $(BIN)
	env CS_AUTH=auth-v2 env/bin/python boto_test.py

test-auth-v4: $(DEPS) $(BIN)
	env CS_AUTH=auth-v4 env/bin/python boto_test.py

clean:
	@rm -rf env
