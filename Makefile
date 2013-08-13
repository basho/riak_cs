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

.PHONY: rel stagedevrel deps test

all: deps compile

compile:
	@./rebar compile

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

clean:
	@./rebar clean

distclean: clean
	@./rebar delete-deps
	@rm -rf $(PKG_ID).tar.gz

test: all
	@./rebar skip_deps=true eunit

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
stage : rel
	$(foreach dep,$(wildcard deps/*), rm -rf rel/riak-cs/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) rel/riak-cs/lib;)
	$(foreach app,$(wildcard apps/*), rm -rf rel/riak-cs/lib/$(shell basename $(app))-* && ln -sf $(abspath $(app)) rel/riak-cs/lib;)

devrel: dev1 dev2 dev3 dev4

dev1 dev2 dev3 dev4: all
	mkdir -p dev
	(cd rel && ../rebar generate skip_deps=true target_dir=../dev/$@ overlay_vars=vars/$@_vars.config)

stagedevrel: dev1 dev2 dev3 dev4
	$(foreach dev,$^, \
	  $(foreach app,$(wildcard apps/*), rm -rf dev/$(dev)/lib/$(shell basename $(app))-* && ln -sf $(abspath $(app)) dev/$(dev)/lib;) \
	  $(foreach dep,$(wildcard deps/*), rm -rf dev/$(dev)/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) dev/$(dev)/lib;))

rtdevrel: rtdev1 rtdev2 rtdev3 rtdev4 rtdev5 rtdev6

rtdev1 rtdev2 rtdev3 rtdev4 rtdev5 rtdev6:
	$(if $(RT_TARGET_CS),,$(error "RT_TARGET_CS var not set, see riak_test/ directory for details"))
	mkdir -p $(RT_TARGET_CS)/$@
	 (cd rel && ../rebar generate skip_deps=true target_dir=$(RT_TARGET_CS)/$@ overlay_vars=vars/$@_vars.config)


devclean: clean
	rm -rf dev

##
## Doc targets
##
docs:
	./rebar skip_deps=true doc

orgs: orgs-doc orgs-README

orgs-doc:
	@emacs -l orgbatch.el -batch --eval="(riak-export-doc-dir \"doc\" 'html)"

orgs-README:
	@emacs -l orgbatch.el -batch --eval="(riak-export-doc-file \"README.org\" 'ascii)"
	@mv README.txt README

APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler
PLT = $(HOME)/.riak-cs_dialyzer_plt

check_plt: compile
	dialyzer --check_plt --plt $(PLT) --apps $(APPS)

build_plt: compile
	dialyzer --build_plt --output_plt $(PLT) --apps $(APPS)

dialyzer: compile
	@echo
	@echo Use "'make check_plt'" to check PLT prior to using this target.
	@echo Use "'make build_plt'" to build PLT prior to using this target.
	@echo
	@sleep 1
	dialyzer -Wno_return -Wunmatched_returns \
		--plt $(PLT) deps/*/ebin ebin | \
	    tee .dialyzer.raw-output | egrep -v -f ./dialyzer.ignore-warnings

cleanplt:
	@echo
	@echo "Are you sure?  It takes about 1/2 hour to re-build."
	@echo Deleting $(PLT) in 5 seconds.
	@echo
	sleep 5
	rm $(PLT)

xref: compile
	./rebar xref skip_deps=true | grep -v unused | egrep -v -f ./xref.ignore-warnings

##
## Packaging targets
##
.PHONY: package
export PKG_VERSION PKG_ID PKG_BUILD BASE_DIR ERLANG_BIN REBAR OVERLAY_VARS RELEASE RIAK_CS_EE_DEPS

package.src: deps
	mkdir -p package
	rm -rf package/$(PKG_ID)
	git archive --format=tar --prefix=$(PKG_ID)/ $(PKG_REVISION)| (cd package && tar -xf -)
	cp rebar.config.script package/$(PKG_ID)
	make -C package/$(PKG_ID) deps
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
