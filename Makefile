REPO		?= riak_cs
HEAD_REVISION   ?= $(shell git describe --tags --exact-match HEAD 2>/dev/null)
PKG_REVISION    ?= $(shell git describe --tags 2>/dev/null)
PKG_VERSION     ?= $(shell git describe --tags | tr - .)
PKG_ID           = riak-cs-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl 2>/dev/null) 2>/dev/null)
OTP_VER          = $(shell echo $(ERLANG_BIN) | rev | cut -d "/" -f 2 | rev)
REBAR           ?= $(BASE_DIR)/rebar3
REL_DIR         ?= _build/default/rel
CS_HTTP_PORT    ?= 8080
PULSE_TESTS      = riak_cs_get_fsm_pulse

.PHONY: rel stagedevrel deps test depgraph graphviz all compile

all: compile

compile:
	@($(REBAR) compile)

deps:
	$(if $(HEAD_REVISION),$(warning "Warning: you have checked out a tag ($(HEAD_REVISION)) and should use the compile target"))
	$(REBAR) upgrade

clean:
	$(REBAR) clean

distclean: clean devclean relclean
	@rm -rf _build

##
## Release targets
##
rel: compile
	rm -rf _build/rel/rel/riak-cs
	$(REBAR) as rel release
	cp -a _build/rel/rel/riak-cs rel/

rel-rpm: compile
	$(REBAR) as rpm release
	cp -a _build/rpm/rel/riak-cs rel/

rel-deb: compile
	$(REBAR) as deb release
	cp -a _build/deb/rel/riak-cs rel/

relclean:
	rm -rf $(REL_DIR)
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
	rel/gen_dev dev$* rel/vars/dev_vars.config.src rel/vars/$*_vars.config
	$(REBAR) as dev release -o dev/dev$* --overlay_vars rel/vars/$*_vars.config

stagedev% : all
	rel/gen_dev dev$* rel/vars/dev_vars.config.src rel/vars/$*_vars.config
	$(REBAR) as dev release -o dev/dev$* --overlay_vars rel/vars/$*_vars.config

devclean: clean
	rm -rf dev

stage : rel
	$(foreach app,$(wildcard apps/*), rm -rf rel/riak-cs/lib/$(shell basename $(app))* && ln -sf $(abspath $(app)) rel/riak-cs/lib;)
	$(foreach dep,$(wildcard deps/*), rm -rf rel/riak-cs/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) rel/riak-cs/lib;)


DIALYZER_APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	webtool eunit syntax_tools compiler
PLT ?= $(HOME)/.riak-cs_dialyzer_plt


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
	@ERLOPTS="-D PULSE" $(REBAR) eunit --module=$(PULSE_TESTS)

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
