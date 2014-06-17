REPO		?= riak_cs
PKG_REVISION    ?= $(shell git describe --tags)
PKG_VERSION	?= $(shell git describe --tags | tr - .)
PKG_ID           = riak-cs-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/rebar
OVERLAY_VARS    ?=
CS_HTTP_PORT    ?= 8080
PULSE_TESTS = riak_cs_get_fsm_pulse

.PHONY: rel stagedevrel deps test depgraph graphviz all compile

all: compile

compile: deps
	@(./rebar compile)

compile-client-test: all
	@./rebar client_test_compile

compile-int-test: all
	@./rebar int_test_compile

compile-riak-test: all
	@./rebar skip_deps=true riak_test_compile
	## There are some Riak CS internal modules that our riak_test
	## test would like to use.  But riak_test doesn't have a nice
	## way of adding the -pz argument + code path that we need.
	## So we'll copy the BEAM files to a place that riak_test is
	## already using.
	cp -v ebin/riak_cs_wm_utils.beam riak_test/ebin

clean-client-test:
	@./rebar client_test_clean

clean-int-test:
	@./rebar int_test_clean

clean-riak-test:
	@./rebar riak_test_clean

deps:
	@./rebar get-deps

##
## Lock Targets
##
##  see https://github.com/seth/rebar_lock_deps_plugin
lock: deps compile
	./rebar lock-deps

locked-all: locked-deps compile

locked-deps:
	@echo "Using rebar.config.lock file to fetch dependencies"
	./rebar -C rebar.config.lock get-deps

clean:
	@./rebar clean

distclean: clean
	@./rebar delete-deps
	@rm -rf $(PKG_ID).tar.gz

## Create a dependency graph png
depgraph: graphviz
	@echo "Note: If you have nothing in deps/ this might be boring"
	@echo "Creating dependency graph..."
	@misc/mapdeps.erl | dot -Tpng -oriak-cs.png
	@echo "Dependency graph created as riak-cs.png"
graphviz:
	$(if $(shell which dot),,$(error "To make the depgraph, you need graphviz installed"))

pulse: all
	@rm -rf $(BASE_DIR)/.eunit
	@./rebar -D PULSE eunit skip_deps=true suites=$(PULSE_TESTS)

test-client: test-clojure test-python test-erlang test-ruby test-php

test-python:
	@cd client_tests/python/ && make CS_HTTP_PORT=$(CS_HTTP_PORT)

test-ruby:
	@bundle --gemfile client_tests/ruby/Gemfile --path vendor
	@cd client_tests/ruby && bundle exec rake spec

test-erlang: compile-client-test
	@./rebar skip_deps=true client_test_run

test-clojure:
	@command -v lein >/dev/null 2>&1 || { echo >&2 "I require lein but it's not installed. \
	Please read client_tests/clojure/clj-s3/README."; exit 1; }
	@cd client_tests/clojure/clj-s3 && lein do deps, midje

test-php:
	@cd client_tests/php && make

test-int: compile-int-test
	@./rebar skip_deps=true int_test_run

##
## Release targets
##
rel: deps compile
	@./rebar generate skip_deps=true $(OVERLAY_VARS)

relclean:
	rm -rf rel/riak-cs

##
## Developer targets
##
##  devN - Make a dev build for node N
##  stagedevN - Make a stage dev build for node N (symlink libraries)
##  devrel - Make a dev build for 1..$DEVNODES
##  stagedevrel Make a stagedev build for 1..$DEVNODES
##
##  Example, make a 68 node devrel cluster
##    make stagedevrel DEVNODES=68

.PHONY : stagedevrel devrel
DEVNODES ?= 6

# 'seq' is not available on all *BSD, so using an alternate in awk
SEQ = $(shell awk 'BEGIN { for (i = 1; i < '$(DEVNODES)'; i++) printf("%i ", i); print i ;exit(0);}')

$(eval stagedevrel : $(foreach n,$(SEQ),stagedev$(n)))
$(eval devrel : $(foreach n,$(SEQ),dev$(n)))

dev% : all
	mkdir -p dev
	rel/gen_dev $@ rel/vars/dev_vars.config.src rel/vars/$@_vars.config
	(cd rel && ../rebar generate target_dir=../dev/$@ overlay_vars=vars/$@_vars.config)

stagedev% : dev%
	$(foreach app,$(wildcard apps/*), rm -rf dev/$^/lib/$(shell basename $(app))* && ln -sf $(abspath $(app)) dev/$^/lib;)
	$(foreach dep,$(wildcard deps/*), rm -rf dev/$^/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) dev/$^/lib;)

devclean: clean
	rm -rf dev

stage : rel
	$(foreach app,$(wildcard apps/*), rm -rf rel/riak-cs/lib/$(shell basename $(app))* && ln -sf $(abspath $(app)) rel/riak-cs/lib;)
	$(foreach dep,$(wildcard deps/*), rm -rf rel/riak-cs/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) rel/riak-cs/lib;)


##
## Doc targets
##
orgs: orgs-doc orgs-README

orgs-doc:
	@emacs -l orgbatch.el -batch --eval="(riak-export-doc-dir \"doc\" 'html)"

orgs-README:
	@emacs -l orgbatch.el -batch --eval="(riak-export-doc-file \"README.org\" 'ascii)"
	@mv README.txt README

DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	webtool eunit syntax_tools compiler
PLT ?= $(HOME)/.riak-cs_dialyzer_plt

##
## Packaging targets
##
.PHONY: package
export PKG_VERSION PKG_ID PKG_BUILD BASE_DIR ERLANG_BIN REBAR OVERLAY_VARS RELEASE

## Do not export RIAK_CS_EE_DEPS unless it is set, since even an empty
## variable will affect the build and 'export' by default makes it empty
## if it is unset
BUILD_EE = $(shell test -n "$${RIAK_CS_EE_DEPS+x}" && echo "true" || echo "false")
ifeq ($(BUILD_EE),true)
export RIAK_CS_EE_DEPS=true
endif


package.src: deps
	mkdir -p package
	rm -rf package/$(PKG_ID)
	git archive --format=tar --prefix=$(PKG_ID)/ $(PKG_REVISION)| (cd package && tar -xf -)
	cp rebar.config.script package/$(PKG_ID)
	make -C package/$(PKG_ID) deps
	mkdir -p package/$(PKG_ID)/priv
	git --git-dir=.git describe --tags >package/$(PKG_ID)/priv/vsn.git
	for dep in package/$(PKG_ID)/deps/*; do \
             echo "Processing dep: $${dep}"; \
             mkdir -p $${dep}/priv; \
             git --git-dir=$${dep}/.git describe --tags >$${dep}/priv/vsn.git; \
        done
	find package/$(PKG_ID) -depth -name ".git" -exec rm -rf {} \;
	tar -C package -czf package/$(PKG_ID).tar.gz $(PKG_ID)

dist: package.src
	cp package/$(PKG_ID).tar.gz .

package: package.src
	make -C package -f $(PKG_ID)/deps/node_package/Makefile

pkgclean: distclean
	rm -rf package

include tools.mk
