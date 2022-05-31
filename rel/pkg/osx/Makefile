export

BUILDDIR        = $(shell pwd)
BUILD_STAGE_DIR = $(BUILDDIR)/riak_cs
PKGNAME         = riak_cs-$(PKG_ID)

default: buildrel
	mkdir ../packages
	tar -czf ../packages/$(PKGNAME).tar.gz -C rel riak-cs && \
	(cd ../packages && shasum -a 256 $(PKGNAME).tar.gz > $(PKGNAME).tar.gz.sha)

buildrel:
	tar -xf $(BASE_DIR)/rel/pkg/out/$(PKG_ID).tar.gz -C $(BASE_DIR)/rel/pkg/out/$(PKGNAME)
	$(MAKE) -C $(BASE_DIR)/rel/pkg/out/$(PKGNAME) rel-osx
	chmod 0755 rel/riak-cs/bin/* rel/riak-cs/erts-*/bin/*
