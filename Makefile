REPO		    ?= riak_cs
PKG_REVISION    ?= $(shell git describe --tags)
PKG_VERSION	    ?= $(shell git describe --tags | tr - .)
PKG_ID           = riak-cs-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= rebar3
OVERLAY_VARS    ?=
CS_HTTP_PORT    ?= 8080
PULSE_TESTS      = riak_cs_get_fsm_pulse

.PHONY: rel stagedevrel deps test depgraph graphviz all compile compile-src

all: compile

compile:

compile-src: deps
	@($(REBAR) compile)

compile-client-test: all
	@$(REBAR) client_test_compile

bitcask-downgrade-script: riak_test/src/downgrade_bitcask.erl

clean-client-test:
	@$(REBAR) client_test_clean

deps:
	@$(REBAR) get-deps

##
## Lock Targets
##
##  see https://github.com/seth/rebar_lock_deps_plugin
lock: deps compile
	$(REBAR) lock-deps

locked-all: locked-deps compile

locked-deps:
	@echo "Using rebar.config.lock file to fetch dependencies"
	$(REBAR) -C rebar.config.lock get-deps

clean:
	@$(REBAR) clean

distclean: clean
	@$(REBAR) delete-deps
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
	@$(REBAR) -D PULSE eunit skip_deps=true suites=$(PULSE_TESTS)

test-client: test-clojure test-boto test-ceph test-erlang test-ruby test-php test-go

test-python:
	@cd client_tests/python/ && ${MAKE} CS_HTTP_PORT=$(CS_HTTP_PORT)

test-boto:
	@cd client_tests/python/ && ${MAKE} boto_tests CS_HTTP_PORT=$(CS_HTTP_PORT)

test-ceph:
	@cd client_tests/python/ && ${MAKE} ceph_tests CS_HTTP_PORT=$(CS_HTTP_PORT)

test-ruby:
	@bundle --gemfile client_tests/ruby/Gemfile --path vendor
	@cd client_tests/ruby && bundle exec rake spec

test-erlang: compile-client-test
	@$(REBAR) skip_deps=true client_test_run

test-clojure:
	@command -v lein >/dev/null 2>&1 || { echo >&2 "I require lein but it's not installed. \
	Please read client_tests/clojure/clj-s3/README."; exit 1; }
	@cd client_tests/clojure/clj-s3 && lein do deps, midje

test-php:
	@cd client_tests/php && ${MAKE}

test-go:
	@cd client_tests/go && ${MAKE}

##
## Release targets
##
rel: deps compile
	@$(REBAR) generate skip_deps=true $(OVERLAY_VARS)

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
DEVNODES ?= 8

# 'seq' is not available on all *BSD, so using an alternate in awk
SEQ = $(shell awk 'BEGIN { for (i = 1; i < '$(DEVNODES)'; i++) printf("%i ", i); print i ;exit(0);}')

$(eval stagedevrel : $(foreach n,$(SEQ),stagedev$(n)))
$(eval devrel : $(foreach n,$(SEQ),dev$(n)))

dev% : all
	mkdir -p dev
	rel/gen_dev $@ rel/vars/dev_vars.config.src rel/vars/$@_vars.config
	(cd rel && $(REBAR) generate target_dir=../dev/$@ overlay_vars=vars/$@_vars.config)

stagedev% : dev%
	$(foreach app,$(wildcard apps/*), rm -rf dev/$^/lib/$(shell basename $(app))* && ln -sf $(abspath $(app)) dev/$^/lib;)
	$(foreach dep,$(wildcard deps/*), rm -rf dev/$^/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) dev/$^/lib;)

devclean: clean
	rm -rf dev

stage : rel
	$(foreach app,$(wildcard apps/*), rm -rf rel/riak-cs/lib/$(shell basename $(app))* && ln -sf $(abspath $(app)) rel/riak-cs/lib;)
	$(foreach dep,$(wildcard deps/*), rm -rf rel/riak-cs/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) rel/riak-cs/lib;)


DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	webtool eunit syntax_tools compiler
PLT ?= $(HOME)/.riak-cs_dialyzer_plt

##
## Packaging targets
##
.PHONY: package
export PKG_VERSION PKG_ID PKG_BUILD BASE_DIR ERLANG_BIN REBAR OVERLAY_VARS RELEASE

package.src: deps
	mkdir -p package
	rm -rf package/$(PKG_ID)
	git archive --format=tar --prefix=$(PKG_ID)/ $(PKG_REVISION)| (cd package && tar -xf -)
	cp pkg.vars.config package/$(PKG_ID)
	${MAKE} -C package/$(PKG_ID) deps
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
	${MAKE} -C package -f $(PKG_ID)/deps/node_package/Makefile

pkgclean: distclean
	rm -rf package

include tools.mk
