REPO		?= riak_moss
RIAK_MOSS_TAG	 = $(shell git describe --tags)
REVISION	?= $(shell echo $(RIAK_MOSS_TAG) | sed -e 's/^$(REPO)-//')
PKG_VERSION	?= $(shell echo $(REVISION) | tr - .)


.PHONY: rel deps

all: deps
	@./rebar compile

deps:
	@./rebar get-deps

clean:
	@./rebar clean

distclean: clean
	@./rebar delete-deps

docs:
	@erl -noshell -run edoc_run application '$(REPO)' '"."' '[]'

test:
	@./rebar skip_deps=true eunit

##
## Release targets
##
rel: deps
	@./rebar compile generate

relclean:
	rm -rf rel/riak_moss

##
## Developer targets
##
stage : rel
	$(foreach dep,$(wildcard deps/*), rm -rf rel/riak_moss/lib/$(shell basename $(dep))-* && ln -sf $(abspath $(dep)) rel/riak_moss/lib;)
	$(foreach app,$(wildcard apps/*), rm -rf rel/riak_moss/lib/$(shell basename $(app))-* && ln -sf $(abspath $(app)) rel/riak_moss/lib;)

##
## Doc targets
##
docs:
	./rebar skip_deps=true doc
	@cp -R apps/webmachine/doc doc/webmachine

orgs: orgs-doc orgs-README

orgs-doc:
	@emacs -l orgbatch.el -batch --eval="(riak-export-doc-dir \"doc\" 'html)"

orgs-README:
	@emacs -l orgbatch.el -batch --eval="(riak-export-doc-file \"README.org\" 'ascii)"
	@mv README.txt README

APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool snmp public_key mnesia eunit syntax_tools compiler
PLT = $(HOME)/.riak_moss_dialyzer_plt

check_plt: compile
	dialyzer --check_plt --plt $(PLT) --apps $(APPS) \
		deps/*/ebin

build_plt: compile
	dialyzer --build_plt --output_plt $(PLT) --apps $(APPS) \
		deps/*/ebin

dialyzer: compile
	@echo
	@echo Use "'make check_plt'" to check PLT prior to using this target.
	@echo Use "'make build_plt'" to build PLT prior to using this target.
	@echo
	@sleep 1
	dialyzer -Wno_return --plt $(PLT) deps/*/ebin | \
	    fgrep -v -f ./dialyzer.ignore-warnings

cleanplt:
	@echo
	@echo "Are you sure?  It takes about 1/2 hour to re-build."
	@echo Deleting $(PLT) in 5 seconds.
	@echo
	sleep 5
	rm $(PLT)

# Release tarball creation
# Generates a tarball that includes all the deps sources so no checkouts are necessary
archivegit = git archive --format=tar --prefix=$(1)/ HEAD | (cd $(2) && tar xf -)
archive = $(call archivegit,$(1),$(2))

buildtar = mkdir distdir && \
		 git clone . distdir/riak-moss-clone && \
		 cd distdir/riak-moss-clone && \
		 git checkout $(RIAK_MOSS_TAG) && \
		 $(call archive,$(RIAK_MOSS_TAG),..) && \
		 mkdir ../$(RIAK_MOSS_TAG)/deps && \
		 make deps; \
		 for dep in deps/*; do \
                     cd $${dep} && \
                     $(call archive,$${dep},../../../$(RIAK_MOSS_TAG)) && \
                     mkdir -p ../../../$(RIAK_MOSS_TAG)/$${dep}/priv && \
                     git describe --tags > ../../../$(RIAK_MOSS_TAG)/$${dep}/priv/vsn.git && \
                     cd ../..; done

distdir:
	$(if $(RIAK_MOSS_TAG), $(call buildtar), $(error "You can't generate a release tarball from a non-tagged revision. Run 'git checkout <tag>', then 'make dist'"))

dist $(RIAK_MOSS_TAG).tar.gz: distdir
	cd distdir; \
	tar czf ../$(RIAK_MOSS_TAG).tar.gz $(RIAK_MOSS_TAG)

ballclean:
	rm -rf $(RIAK_MOSS_TAG).tar.gz distdir

package: dist
	$(MAKE) -C package package

pkgclean:
	$(MAKE) -C package pkgclean

.PHONY: package
export PKG_VERSION REPO REVISION RIAK_MOSS_TAG

