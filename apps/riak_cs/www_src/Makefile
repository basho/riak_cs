SUBDIRS = control_ui

all: $(SUBDIRS)

$(SUBDIRS):
	$(MAKE) -C $@ build

install:
	for d in $(SUBDIRS); do $(MAKE) -C $$d install; done

clean:
	for d in $(SUBDIRS); do $(MAKE) -C $$d clean; done

.PHONY: all install clean $(SUBDIRS)
