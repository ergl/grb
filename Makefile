BASEDIR = $(shell pwd)
REBAR = rebar3
RELPATH = _build/default/rel/grb
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

ct_test:
	$(REBAR) ct

devrel1:
	$(REBAR) release -n grb_local1

devrel2:
	$(REBAR) release -n grb_local2

devrel3:
	$(REBAR) release -n grb_local3

devrel: devrel1 devrel2 devrel3

dev1-attach:
	INSTANCE_NAME=grb_local1 RIAK_HANDOFF_PORT=8199 TCP_LIST_PORT=7891 $(BASEDIR)/_build/default/rel/grb_local1/bin/$(ENVFILE) attach

dev2-attach:
	INSTANCE_NAME=grb_local2 RIAK_HANDOFF_PORT=8299 TCP_LIST_PORT=7892 $(BASEDIR)/_build/default/rel/grb_local2/bin/$(ENVFILE) attach

dev3-attach:
	INSTANCE_NAME=grb_local3 RIAK_HANDOFF_PORT=8399 TCP_LIST_PORT=7893 $(BASEDIR)/_build/default/rel/grb_local3/bin/$(ENVFILE) attach

devrel-clean:
	rm -rf _build/default/rel/grb_local*

dev1-start:
	INSTANCE_NAME=grb_local1 RIAK_HANDOFF_PORT=8199 TCP_LIST_PORT=7891 $(BASEDIR)/_build/default/rel/grb_local1/bin/$(ENVFILE) start

dev2-start:
	INSTANCE_NAME=grb_local2 RIAK_HANDOFF_PORT=8299 TCP_LIST_PORT=7892 $(BASEDIR)/_build/default/rel/grb_local2/bin/$(ENVFILE) start

dev3-start:
	INSTANCE_NAME=grb_local3 RIAK_HANDOFF_PORT=8399 TCP_LIST_PORT=7893 $(BASEDIR)/_build/default/rel/grb_local3/bin/$(ENVFILE) start

devrel-start: dev1-start dev2-start dev3-start

dev2-join:
	INSTANCE_NAME=grb_local2 RIAK_HANDOFF_PORT=8299 TCP_LIST_PORT=7892 $(BASEDIR)/_build/default/rel/grb_local2/bin/$(ENVFILE) eval 'riak_core:join("grb_local1@127.0.0.1")'

dev3-join:
	INSTANCE_NAME=grb_local3 RIAK_HANDOFF_PORT=8399 TCP_LIST_PORT=7893 $(BASEDIR)/_build/default/rel/grb_local3/bin/$(ENVFILE) eval 'riak_core:join("grb_local1@127.0.0.1")'

devrel-join: dev2-join dev3-join

devrel-cluster-plan:
	INSTANCE_NAME=grb_local1 RIAK_HANDOFF_PORT=8199 TCP_LIST_PORT=7891 $(BASEDIR)/_build/default/rel/grb_local1/bin/$(ENVFILE) eval 'riak_core_claimant:plan()'

devrel-cluster-commit:
	INSTANCE_NAME=grb_local1 RIAK_HANDOFF_PORT=8199 TCP_LIST_PORT=7891 $(BASEDIR)/_build/default/rel/grb_local1/bin/$(ENVFILE) eval 'riak_core_claimant:commit()'

devrel-status:
	INSTANCE_NAME=grb_local1 RIAK_HANDOFF_PORT=8199 TCP_LIST_PORT=7891 $(BASEDIR)/_build/default/rel/grb_local1/bin/$(ENVFILE) eval 'riak_core_console:member_status([])'

dev1-ping:
	INSTANCE_NAME=grb_local1 RIAK_HANDOFF_PORT=8199 TCP_LIST_PORT=7891 $(BASEDIR)/_build/default/rel/grb_local1/bin/$(ENVFILE) ping

dev2-ping:
	INSTANCE_NAME=grb_local2 RIAK_HANDOFF_PORT=8299 TCP_LIST_PORT=7892 $(BASEDIR)/_build/default/rel/grb_local2/bin/$(ENVFILE) ping

dev3-ping:
	INSTANCE_NAME=grb_local3 RIAK_HANDOFF_PORT=8399 TCP_LIST_PORT=7893 $(BASEDIR)/_build/default/rel/grb_local3/bin/$(ENVFILE) ping

devrel-ping: dev1-ping dev2-ping dev3-ping

dev1-stop:
	INSTANCE_NAME=grb_local1 RIAK_HANDOFF_PORT=8199 TCP_LIST_PORT=7891 $(BASEDIR)/_build/default/rel/grb_local1/bin/$(ENVFILE) stop

dev2-stop:
	INSTANCE_NAME=grb_local2 RIAK_HANDOFF_PORT=8299 TCP_LIST_PORT=7892 $(BASEDIR)/_build/default/rel/grb_local2/bin/$(ENVFILE) stop

dev3-stop:
	INSTANCE_NAME=grb_local3 RIAK_HANDOFF_PORT=8399 TCP_LIST_PORT=7893 $(BASEDIR)/_build/default/rel/grb_local3/bin/$(ENVFILE) stop

devrel-stop: dev1-stop dev2-stop dev3-stop
