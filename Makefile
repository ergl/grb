BASEDIR = $(shell pwd)
REBAR = rebar3
RELPATH = _build/default/rel/grb
PRODRELPATH = _build/prod/rel/grb
DEV1RELPATH = _build/dev1/rel/grb
DEV2RELPATH = _build/dev2/rel/grb
DEV3RELPATH = _build/dev3/rel/grb
APPNAME = grb
ENVFILE = env
SHELL = /bin/bash

.PHONY: test

all: compile

compile:
	$(REBAR) compile

clean:
	$(REBAR) clean --all

rel: compile
	$(REBAR) release -n grb
	mkdir -p $(RELPATH)/../grb_config

console:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) console

relclean:
	rm -rf _build/default/rel

start:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) start

ping:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) ping

stop:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) stop

attach:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) attach

test:
	${REBAR} eunit skip_deps=true

prod-release:
	$(REBAR) as prod release
	mkdir -p $(PRODRELPATH)/../grb_config

prod-console:
	cd $(PRODRELPATH) && ./bin/grb console

ct_test:
	$(REBAR) ct

devrel1:
	$(REBAR) as dev1 release
	mkdir -p $(DEV1RELPATH)/../grb_config

devrel2:
	$(REBAR) as dev2 release
	mkdir -p $(DEV2RELPATH)/../grb_config

devrel3:
	$(REBAR) as dev3 release
	mkdir -p $(DEV3RELPATH)/../grb_config

devrel: devrel1 devrel2 devrel3

dev1-attach:
	$(BASEDIR)/_build/dev1/rel/grb/bin/$(APPNAME) attach

dev2-attach:
	$(BASEDIR)/_build/dev2/rel/grb/bin/$(APPNAME) attach

dev3-attach:
	$(BASEDIR)/_build/dev3/rel/grb/bin/$(APPNAME) attach

dev1-console:
	$(BASEDIR)/_build/dev1/rel/grb/bin/$(APPNAME) console

dev2-console:
	$(BASEDIR)/_build/dev2/rel/grb/bin/$(APPNAME) console

dev3-console:
	$(BASEDIR)/_build/dev3/rel/grb/bin/$(APPNAME) console

devrel-clean:
	rm -rf _build/dev*/rel

devrel-start:
	for d in $(BASEDIR)/_build/dev*; do $$d/rel/grb/bin/$(APPNAME) start; done

devrel-join:
	for d in $(BASEDIR)/_build/dev{2,3}; do $$d/rel/grb/bin/$(APPNAME) eval 'riak_core:join("grb1@127.0.0.1")'; done

devrel-cluster-plan:
	$(BASEDIR)/_build/dev1/rel/grb/bin/$(APPNAME) eval 'riak_core_claimant:plan()'

devrel-cluster-commit:
	$(BASEDIR)/_build/dev1/rel/grb/bin/$(APPNAME) eval 'riak_core_claimant:commit()'

devrel-status:
	$(BASEDIR)/_build/dev1/rel/grb/bin/$(APPNAME) eval 'riak_core_console:member_status([])'

devrel-ping:
	for d in $(BASEDIR)/_build/dev*; do $$d/rel/grb/bin/$(APPNAME) ping; true; done

devrel-stop:
	for d in $(BASEDIR)/_build/dev*; do $$d/rel/grb/bin/$(APPNAME) stop; true; done
