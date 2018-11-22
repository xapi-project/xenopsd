include config.mk

OPAM_PREFIX=$(DESTDIR)$(shell opam config var prefix)
OPAM_LIBDIR=$(DESTDIR)$(shell opam config var lib)

.PHONY: build clean release test reindent install uninstall doc

release:
	dune build @install --profile=release

build:
	dune build @install --profile=dev

clean:
	dune clean

test:
	dune runtest --no-buffer --profile=release

reindent:
	git ls-files '*.ml*' '**/*.ml*' | xargs ocp-indent --syntax cstruct -i

#requires odoc
doc:
	dune build @doc --profile=release

install:
	dune install --prefix=$(OPAM_PREFIX) --libdir=$(OPAM_LIBDIR) -p xapi-xenopsd
	install -D _build/install/default/bin/xenopsd-simulator $(DESTDIR)/$(SBINDIR)/xenopsd-simulator
	install -D _build/install/default/man/man1/xenopsd-simulator.1 $(DESTDIR)/$(MANDIR)/man1/xenopsd-simulator.1
	install -D _build/install/default/bin/xenopsd-xc $(DESTDIR)/$(SBINDIR)/xenopsd-xc
	install -D _build/install/default/bin/fence.bin $(DESTDIR)/$(OPTDIR)/fence.bin
	install -D _build/install/default/man/man1/xenopsd-xc.1 $(DESTDIR)/$(MANDIR)/man1/xenopsd-xc.1
	install -D _build/install/default/bin/set-domain-uuid $(DESTDIR)/$(LIBEXECDIR)/set-domain-uuid
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
	install -D ./scripts/send_message.py $(DESTDIR)/$(LIBEXECDIR)/send_message.py
	install -D ./scripts/qemu-wrapper $(DESTDIR)/$(QEMU_WRAPPER_DIR)/qemu-wrapper
	DESTDIR=$(DESTDIR) SBINDIR=$(SBINDIR) QEMU_WRAPPER_DIR=$(QEMU_WRAPPER_DIR) LIBEXECDIR=$(LIBEXECDIR) ETCDIR=$(ETCDIR) ./scripts/make-custom-xenopsd.conf

uninstall:
	dune uninstall --prefix=$(OPAM_PREFIX) --libdir=$(OPAM_LIBDIR) -p xapi-xenopsd
	rm -f $(DESTDIR)/$(SBINDIR)/xenopsd-xc
	rm -f $(DESTDIR)/$(OPTDIR)/fence.bin
	rm -f $(DESTDIR)/$(SBINDIR)/xenopsd-simulator
	rm -f $(DESTDIR)/$(MANDIR)/man1/xenopsd-xc.1
	rm -f $(DESTDIR)/$(MANDIR)/man1/xenopsd-simulator.1
	rm -f $(DESTDIR)/$(LIBEXECDIR)/set-domain-uuid
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
	rm -f $(DESTDIR)/$(LIBEXECDIR)/send_message.py*
	rm -f $(DESTDIR)/$(QEMU_WRAPPER_DIR)/qemu-wrapper

.DEFAULT_GOAL := release
