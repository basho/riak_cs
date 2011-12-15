REPO		?= riak_moss
PKG_NAME        ?= riak-moss
PKG_REVISION    ?= $(shell git describe --tags)
PKG_VERSION	?= $(shell git describe --tags | tr - .)
PKG_ID           = $(PKG_NAME)-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl))
REBAR           ?= $(BASE_DIR)/rebar
OVERLAY_VARS    ?=

.PHONY: rel deps test

all: deps compile

compile:
	@./rebar compile

deps:
	@./rebar get-deps

clean:
	@./rebar clean

distclean: clean
	@./rebar delete-deps

test:
	@./rebar skip_deps=true eunit

parity-test:
	@python test/prototype_parity.py -v

##
## Release targets
##
rel: deps compile
	@./rebar compile generate

relclean:
	rm -rf rel/riak_moss

##
## Developer targets
##
stage : rel
	$(foreach dep,$(wildcard deps/*), rm -rf rel/riak_moss/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) rel/riak_moss/lib;)
	$(foreach app,$(wildcard apps/*), rm -rf rel/riak_moss/lib/$(shell basename $(app))-* && ln -sf $(abspath $(app)) rel/riak_moss/lib;)

devrel:
	mkdir -p dev
	(cd rel && ../rebar generate target_dir=../dev/$(REPO) overlay_vars=vars/dev_vars.config)

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
PLT = $(HOME)/.riak_moss_dialyzer_plt

check_plt: compile
	dialyzer --check_plt --plt $(PLT) --apps $(APPS) \
		deps/*/ebin ebin

build_plt: compile
	dialyzer --build_plt --output_plt $(PLT) --apps $(APPS) \
		deps/*/ebin ebin

dialyzer: compile
	@echo
	@echo Use "'make check_plt'" to check PLT prior to using this target.
	@echo Use "'make build_plt'" to build PLT prior to using this target.
	@echo
	@sleep 1
	dialyzer -Wno_return --plt $(PLT) deps/*/ebin ebin | \
	    fgrep -v -f ./dialyzer.ignore-warnings

cleanplt:
	@echo
	@echo "Are you sure?  It takes about 1/2 hour to re-build."
	@echo Deleting $(PLT) in 5 seconds.
	@echo
	sleep 5
	rm $(PLT)

##
## Packaging targets
##
.PHONY: package
export PKG_NAME PKG_VERSION PKG_ID PKG_BUILD BASE_DIR ERLANG_BIN REBAR OVERLAY_VARS RELEASE

package.src: deps
	mkdir -p package
	rm -rf package/$(PKG_ID)
	git archive --format=tar --prefix=$(PKG_ID)/ $(PKG_REVISION)| (cd package && tar -xf -)
	make -C package/$(PKG_ID) deps
	for dep in package/$(PKG_ID)/deps/*; do \
             echo "Processing dep: ${{dep}"; \
             mkdir -p $${dep}/priv; \
             git --git-dir=$${dep}/.git describe --tags >$${dep}/priv/vsn.git; \
        done
	find package/$(PKG_ID) -name ".git" -depth -exec rm -rf {} \;
	tar -C package -czf package/$(PKG_ID).tar.gz $(PKG_ID)


package.%: package.src
	make -C package -f $(PKG_ID)/deps/node_package/priv/templates/$*/Makefile.bootstrap

pkgclean: distclean
	rm -rf package
