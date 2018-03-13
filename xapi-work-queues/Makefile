OPAM_PREFIX=$(DESTDIR)$(shell opam config var prefix)
OPAM_LIBDIR=$(DESTDIR)$(shell opam config var lib)

.PHONY: build clean release test reindent install uninstall

build:
	jbuilder build @install --dev

clean:
	jbuilder clean

release:
	jbuilder build @install

test:
	jbuilder runtest --no-buffer

reindent:
	git ls-files '*.ml*' '**/*.ml*' | xargs ocp-indent --syntax cstruct -i

#requires odoc
doc:
	jbuilder build @doc

install:
	jbuilder install --prefix=$(OPAM_PREFIX) --libdir=$(OPAM_LIBDIR) xapi-work-queues

uninstall:
	jbuilder uninstall --prefix=$(OPAM_PREFIX) --libdir=$(OPAM_LIBDIR) xapi-work-queues

.DEFAULT_GOAL := release
