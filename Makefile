include config.mk

.PHONY: build clean release test format install uninstall doc

release:
	dune build @install --profile=release

build:
	dune build @install --profile=dev

clean:
	dune clean

test:
	dune runtest --no-buffer --profile=release

format:
	dune build @fmt --auto-promote

#requires odoc
doc:
	dune build @doc --profile=release

install:
	dune install -p xapi-xenopsd
	install -D _build/install/default/bin/xenopsd-simulator $(DESTDIR)/$(SBINDIR)/xenopsd-simulator
	install -D _build/install/default/man/man1/xenopsd-simulator.1.gz $(DESTDIR)/$(MANDIR)/man1/xenopsd-simulator.1.gz
	install -D _build/install/default/bin/xenopsd-xc $(DESTDIR)/$(SBINDIR)/xenopsd-xc
	install -D _build/install/default/bin/fence.bin $(DESTDIR)/$(OPTDIR)/fence.bin
	install -D _build/install/default/man/man1/xenopsd-xc.1.gz $(DESTDIR)/$(MANDIR)/man1/xenopsd-xc.1.gz
	install -D _build/install/default/bin/set-domain-uuid $(DESTDIR)/$(LIBEXECDIR)/set-domain-uuid
	install -D _build/install/default/bin/xenops-cli $(DESTDIR)/$(SBINDIR)/xenops-cli
	install -D _build/install/default/man/man1/xenops-cli.1.gz $(DESTDIR)/$(MANDIR)/man1/xenops-cli.1.gz
	install -D _build/install/default/bin/list_domains $(DESTDIR)/$(BINDIR)/list_domains
	install -D ./scripts/vif $(DESTDIR)/$(LIBEXECDIR)/vif
	install -D ./scripts/vif-real $(DESTDIR)/$(LIBEXECDIR)/vif-real
	install -D ./scripts/block $(DESTDIR)/$(LIBEXECDIR)/block
	install -D ./scripts/xen-backend.rules $(DESTDIR)/$(ETCDIR)/udev/rules.d/xen-backend.rules
	install -D ./scripts/tap $(DESTDIR)/$(LIBEXECDIR)/tap
	install -D ./scripts/qemu-dm-wrapper $(DESTDIR)/$(LIBEXECDIR)/qemu-dm-wrapper
	install -D ./scripts/qemu-vif-script $(DESTDIR)/$(LIBEXECDIR)/qemu-vif-script
	install -D ./scripts/setup-vif-rules $(DESTDIR)/$(LIBEXECDIR)/setup-vif-rules
	install -D ./scripts/setup-pvs-proxy-rules $(DESTDIR)/$(LIBEXECDIR)/setup-pvs-proxy-rules
	install -D ./scripts/common.py $(DESTDIR)/$(LIBEXECDIR)/common.py
	install -D ./scripts/igmp_query_injector.py $(DESTDIR)/$(LIBEXECDIR)/igmp_query_injector.py
	install -D ./scripts/qemu-wrapper $(DESTDIR)/$(QEMU_WRAPPER_DIR)/qemu-wrapper
	DESTDIR=$(DESTDIR) SBINDIR=$(SBINDIR) QEMU_WRAPPER_DIR=$(QEMU_WRAPPER_DIR) LIBEXECDIR=$(LIBEXECDIR) ETCDIR=$(ETCDIR) ./scripts/make-custom-xenopsd.conf

uninstall:
	dune uninstall -p xapi-xenopsd
	rm -f $(DESTDIR)/$(SBINDIR)/xenopsd-xc
	rm -f $(DESTDIR)/$(OPTDIR)/fence.bin
	rm -f $(DESTDIR)/$(SBINDIR)/xenopsd-simulator
	rm -f $(DESTDIR)/$(MANDIR)/man1/xenopsd-xc.1
	rm -f $(DESTDIR)/$(MANDIR)/man1/xenopsd-simulator.1
	rm -f $(DESTDIR)/$(LIBEXECDIR)/set-domain-uuid
	rm -f $(DESTDIR)/$(SBINDIR)/xenops-cli
	rm -f $(DESTDIR)/$(MANDIR)/man1/xenops-cli.1
	rm -f $(DESTDIR)/$(BINDIR)/list_domains
	rm -f $(DESTDIR)/$(ETCDIR)/xenopsd.conf
	rm -f $(DESTDIR)/$(LIBEXECDIR)/vif
	rm -f $(DESTDIR)/$(LIBEXECDIR)/vif-real
	rm -f $(DESTDIR)/$(LIBEXECDIR)/block
	rm -f $(DESTDIR)/$(ETCDIR)/udev/rules.d/xen-backend.rules
	rm -f $(DESTDIR)/$(LIBEXECDIR)/tap
	rm -f $(DESTDIR)/$(LIBEXECDIR)/qemu-dm-wrapper
	rm -f $(DESTDIR)/$(LIBEXECDIR)/qemu-vif-script
	rm -f $(DESTDIR)/$(LIBEXECDIR)/setup-vif-rules
	rm -f $(DESTDIR)/$(LIBEXECDIR)/setup-pvs-proxy-rules
	rm -f $(DESTDIR)/$(LIBEXECDIR)/common.py*
	rm -f $(DESTDIR)/$(LIBEXECDIR)/igmp_query_injector.py*
	rm -f $(DESTDIR)/$(QEMU_WRAPPER_DIR)/qemu-wrapper

.DEFAULT_GOAL := release
