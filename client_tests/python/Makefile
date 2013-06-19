.PHONY: boto_tests ceph_tests

all:
	$(MAKE) -C ./boto_tests
	$(MAKE) -C ./ceph_tests

boto_tests:
	$(MAKE) -C ./boto_tests

ceph_tests:
	$(MAKE) -C ./ceph_tests

clean:
	$(MAKE) -C ./boto_tests clean
	$(MAKE) -C ./ceph_tests clean
