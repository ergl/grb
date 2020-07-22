BASEDIR = $(shell pwd)
REBAR = $(BASEDIR)/rebar3
RELPATH = _build/default/rel/grb
DEV_PROFILE = debug_basic_replication
DEV1RELPATH = _build/$(DEV_PROFILE)/rel/grb_local1
DEV2RELPATH = _build/$(DEV_PROFILE)/rel/grb_local2
DEV3RELPATH = _build/$(DEV_PROFILE)/rel/grb_local3
DEV4RELPATH = _build/$(DEV_PROFILE)/rel/grb_local4
APPNAME = grb
ENVFILE = env
SHELL = /bin/bash

.PHONY: test

all: compile

compile:
	$(REBAR) compile

check_binaries:
	$(REBAR) as debug_bin compile

xref:
	$(REBAR) xref skip_deps=true

dialyzer:
	$(REBAR) dialyzer

debug:
	$(REBAR) as debug_log compile

cure:
	$(REBAR) as basic_replication compile

clean:
	$(REBAR) clean --all

rel: compile
	$(REBAR) release -n grb

debugrel:
	$(REBAR) as debug_log release -n grb

curerel:
	$(REBAR) as basic_replication release -n grb

debugrel-clean:
	rm -rf _build/debug_log/rel

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
	escript -c bin/join_cluster_script.erl eunit

ct_test:
	$(REBAR) ct

dev1-rel:
	$(REBAR) as $(DEV_PROFILE) release -n grb_local1

dev2-rel:
	$(REBAR) as $(DEV_PROFILE) release -n grb_local2

dev3-rel:
	$(REBAR) as $(DEV_PROFILE) release -n grb_local3

dev4-rel:
	$(REBAR) as $(DEV_PROFILE) release -n grb_local4

devrel: dev1-rel dev2-rel dev3-rel dev4-rel

dev1-attach:
	INSTANCE_NAME=grb_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) attach

dev2-attach:
	INSTANCE_NAME=grb_local2 $(BASEDIR)/$(DEV2RELPATH)/bin/$(ENVFILE) attach

dev3-attach:
	INSTANCE_NAME=grb_local3 $(BASEDIR)/$(DEV3RELPATH)/bin/$(ENVFILE) attach

dev4-attach:
	INSTANCE_NAME=grb_local4 $(BASEDIR)/$(DEV4RELPATH)/bin/$(ENVFILE) attach

devrelclean:
	rm -rf _build/$(DEV_PROFILE)/rel/grb_local*

dev1-start:
	INSTANCE_NAME=grb_local1 RIAK_HANDOFF_PORT=8199 TCP_LIST_PORT=7891 INTER_DC_PORT=8991 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) start

dev2-start:
	INSTANCE_NAME=grb_local2 RIAK_HANDOFF_PORT=8299 TCP_LIST_PORT=7892 INTER_DC_PORT=8992 $(BASEDIR)/$(DEV2RELPATH)/bin/$(ENVFILE) start

dev3-start:
	INSTANCE_NAME=grb_local3 RIAK_HANDOFF_PORT=8399 TCP_LIST_PORT=7893 INTER_DC_PORT=8993 $(BASEDIR)/$(DEV3RELPATH)/bin/$(ENVFILE) start

dev4-start:
	INSTANCE_NAME=grb_local4 RIAK_HANDOFF_PORT=8499 TCP_LIST_PORT=7894 INTER_DC_PORT=8994 $(BASEDIR)/$(DEV4RELPATH)/bin/$(ENVFILE) start

devstart: dev1-start dev2-start dev3-start dev4-start

dev2-join:
	INSTANCE_NAME=grb_local2 $(BASEDIR)/$(DEV2RELPATH)/bin/$(ENVFILE) eval 'riak_core:join("grb_local1@127.0.0.1")'

dev3-join:
	INSTANCE_NAME=grb_local3 $(BASEDIR)/$(DEV3RELPATH)/bin/$(ENVFILE) eval 'riak_core:join("grb_local1@127.0.0.1")'

dev4-join:
	INSTANCE_NAME=grb_local4 $(BASEDIR)/$(DEV4RELPATH)/bin/$(ENVFILE) eval 'riak_core:join("grb_local1@127.0.0.1")'

devjoin: dev2-join dev3-join dev4-join

dev-cluster-plan:
	INSTANCE_NAME=grb_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) eval 'riak_core_claimant:plan()'

dev-cluster-commit:
	INSTANCE_NAME=grb_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) eval 'riak_core_claimant:commit()'

dev-status:
	INSTANCE_NAME=grb_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) eval 'riak_core_console:member_status([])'

dev1-ping:
	INSTANCE_NAME=grb_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) ping

dev2-ping:
	INSTANCE_NAME=grb_local2 $(BASEDIR)/$(DEV2RELPATH)/bin/$(ENVFILE) ping

dev3-ping:
	INSTANCE_NAME=grb_local3 $(BASEDIR)/$(DEV3RELPATH)/bin/$(ENVFILE) ping

dev4-ping:
	INSTANCE_NAME=grb_local4 $(BASEDIR)/$(DEV4RELPATH)/bin/$(ENVFILE) ping

devping: dev1-ping dev2-ping dev3-ping dev4-ping

dev1-stop:
	INSTANCE_NAME=grb_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) stop

dev2-stop:
	INSTANCE_NAME=grb_local2 $(BASEDIR)/$(DEV2RELPATH)/bin/$(ENVFILE) stop

dev3-stop:
	INSTANCE_NAME=grb_local3 $(BASEDIR)/$(DEV3RELPATH)/bin/$(ENVFILE) stop

dev4-stop:
	INSTANCE_NAME=grb_local4 $(BASEDIR)/$(DEV4RELPATH)/bin/$(ENVFILE) stop

devstop: dev1-stop dev2-stop dev3-stop dev4-stop
