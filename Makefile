REPO		?= riak_cs
HEAD_REVISION   ?= $(shell git describe --tags --exact-match HEAD 2>/dev/null)
PKG_REVISION    ?= $(shell git describe --tags 2>/dev/null)
PKG_VERSION     ?= $(shell git describe --tags | tr - .)
PKG_ID           = riak_cs-$(PKG_VERSION)
PKG_BUILD        = 1
BASE_DIR         = $(shell pwd)
ERLANG_BIN       = $(shell dirname $(shell which erl 2>/dev/null) 2>/dev/null)
OTP_VER          = $(shell erl -eval 'erlang:display(erlang:system_info(otp_release)), halt().' -noshell)
REBAR           ?= $(BASE_DIR)/rebar3
CS_HTTP_PORT    ?= 8080
PULSE_TESTS      = riak_cs_get_fsm_pulse

.PHONY: rel stagedevrel deps test depgraph graphviz all compile package pkg-clean

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
	@$(REBAR) as rel release
	@cp -a _build/rel/rel/riak-cs rel/

rel-rpm: compile
	@$(REBAR) as rpm release
	@cp -a _build/rpm/rel/riak-cs rel/

rel-deb: compile
	@$(REBAR) as deb release
	@cp -a _build/deb/rel/riak-cs rel/

rel-fbsdng: compile relclean
	@$(REBAR) as fbsdng release
	@cp -a _build/fbsdng/rel/riak-cs rel/

rel-docker: compile relclean
	@REBAR_CONFIG=rebar.docker.config $(REBAR) release
	@cp -a _build/default/rel/riak-cs rel/

relclean:
	rm -rf _build/default/rel rel/riak-cs

##
## test targets
##
test: eunit proper

eunit:
	$(REBAR) eunit

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
SEQ = $(shell seq $(DEVNODES))

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
	$(foreach app,$(wildcard apps/*),               rm -rf rel/riak-cs/lib/$(shell basename $(app))* && ln -sf $(abspath $(app)) rel/riak-cs/lib;)
	$(foreach dep,$(wildcard _build/default/lib/*), rm -rf rel/riak-cs/lib/$(shell basename $(dep))* && ln -sf $(abspath $(dep)) rel/riak-cs/lib;)


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
## Version and naming variables for distribution and packaging
##

# Tag from git with style <tagname>-<commits_since_tag>-<current_commit_hash>
# Ex: When on a tag:            riak-1.0.3   (no commits since tag)
#     For most normal Commits:  riak-1.1.0pre1-27-g1170096
#                                 Last tag:          riak-1.1.0pre1
#                                 Commits since tag: 27
#                                 Hash of commit:    g1170096
REPO_TAG 	:= $(shell git describe --tags)

# Split off repo name
# Changes to 1.0.3 or 1.1.0pre1-27-g1170096 from example above
REVISION = $(shell echo $(REPO_TAG) | sed -e 's/^$(REPO)-//')

# Primary version identifier, strip off commmit information
# Changes to 1.0.3 or 1.1.0pre1 from example above
MAJOR_VERSION	?= $(shell echo $(REVISION) | sed -e 's/\([0-9.]*\)-.*/\1/')

# Name resulting directory & tar file based on current status of the git tag
# If it is a tagged release (PKG_VERSION == MAJOR_VERSION), use the toplevel
#   tag as the package name, otherwise generate a unique hash of all the
#   dependencies revisions to make the package name unique.
#   This enables the toplevel repository package to change names
#   when underlying dependencies change.
NAME_HASH = $(shell git hash-object distdir/$(CLONEDIR)/$(MANIFEST_FILE) 2>/dev/null | cut -c 1-8)
PKG_ID := "$(REPO_TAG)-OTP$(OTP_VER)"

##
## Packaging targets
##

# Yes another variable, this one is repo-<generatedhash
# which differs from $REVISION that is repo-<commitcount>-<commitsha>
PKG_VERSION = $(shell echo $(PKG_ID) | sed -e 's/^$(REPO)-//')

package:
	mkdir rel/pkg/out/riak_cs-$(PKG_ID)
	git archive --format=tar HEAD | gzip >rel/pkg/out/$(PKG_ID).tar.gz
	$(MAKE) -f rel/pkg/Makefile

packageclean:
	rm -rf rel/pkg/out/*


.PHONY: package
export PKG_VERSION PKG_ID PKG_BUILD BASE_DIR ERLANG_BIN REBAR OVERLAY_VARS RELEASE

# Package up a devrel to save time later rebuilding it
pkg-devrel: devrel
	echo -n $(PKG_REVISION) > VERSION
	tar -czf $(PKG_ID)-devrel.tar.gz dev/ VERSION
	rm -rf VERSION

pkg-rel: rel
	tar -czf $(PKG_ID)-rel.tar.gz -C rel/ .
