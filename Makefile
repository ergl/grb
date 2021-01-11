BASEDIR = $(shell pwd)
REBAR = $(BASEDIR)/rebar3
BASIC_PROFILE = default
DEV_PROFILE = default
RELPATH = _build/$(BASIC_PROFILE)/rel/grb
DEV1RELPATH = _build/$(DEV_PROFILE)/rel/grb_local1
DEV2RELPATH = _build/$(DEV_PROFILE)/rel/grb_local2
DEV3RELPATH = _build/$(DEV_PROFILE)/rel/grb_local3
DEV4RELPATH = _build/$(DEV_PROFILE)/rel/grb_local4
DEVRINGSIZE=4
APPNAME = grb
ENVFILE = env
SHELL = /bin/bash

.PHONY: test

all: compile

compile:
	$(REBAR) as $(BASIC_PROFILE) compile

check_binaries:
	$(REBAR) as debug_bin compile

xref:
	- $(REBAR) xref skip_deps=true
	- $(REBAR) as cure xref skip_deps=true
	- $(REBAR) as ft_cure xref skip_deps=true
	- $(REBAR) as uniform_blue xref skip_deps=true
	- $(REBAR) as redblue_naive xref skip_deps=true

dialyzer:
	- $(REBAR) dialyzer
	- $(REBAR) as cure dialyzer
	- $(REBAR) as ft_cure dialyzer
	- $(REBAR) as uniform_blue dialyzer
	- $(REBAR) as redblue_naive dialyzer

debug:
	$(REBAR) as debug_log compile

cure:
	$(REBAR) as cure compile

ft_cure:
	$(REBAR) as ft_cure compile

uniform:
	$(REBAR) as uniform_blue compile

clean:
	$(REBAR) clean --all

rel: compile
	$(REBAR) as $(BASIC_PROFILE) release -n grb

debugrel:
	$(REBAR) as debug_log release -n grb

curerel:
	$(REBAR) as cure release -n grb

ft_curerel:
	$(REBAR) as ft_cure release -n grb

unirel:
	$(REBAR) as uniform_blue release -n grb

debugrel-clean:
	rm -rf _build/debug_log/rel

console:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) console

relclean:
	rm -rf $(BASEDIR)/$(RELPATH)

start:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) daemon

ping:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) ping

stop:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) stop

attach:
	$(BASEDIR)/$(RELPATH)/bin/$(ENVFILE) daemon_attach

test:
	${REBAR} eunit skip_deps=true
	${REBAR} as cure eunit skip_deps=true
	${REBAR} as ft_cure eunit skip_deps=true
	${REBAR} as uniform_blue eunit skip_deps=true
	${REBAR} as redblue_naive eunit skip_deps=true

ct:
	$(REBAR) ct --fail_fast

full_ct: ct
	$(REBAR) as cure ct
	$(REBAR) as ft_cure ct
	$(REBAR) as uniform_blue ct
	${REBAR} as redblue_naive ct

ct_clean:
	rm -rf $(BASEDIR)/_build/test/logs
	rm -rf $(BASEDIR)/_build/cure+test/logs
	rm -rf $(BASEDIR)/_build/ft_cure+test/logs
	rm -rf $(BASEDIR)/_build/uniform_blue+test/logs
	rm -rf $(BASEDIR)/_build/redblue_naive+test/logs

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
	INSTANCE_NAME=grb_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) daemon_attach

dev2-attach:
	INSTANCE_NAME=grb_local2 $(BASEDIR)/$(DEV2RELPATH)/bin/$(ENVFILE) daemon_attach

dev3-attach:
	INSTANCE_NAME=grb_local3 $(BASEDIR)/$(DEV3RELPATH)/bin/$(ENVFILE) daemon_attach

dev4-attach:
	INSTANCE_NAME=grb_local4 $(BASEDIR)/$(DEV4RELPATH)/bin/$(ENVFILE) daemon_attach

devrelclean:
	rm -rf _build/$(DEV_PROFILE)/rel/grb_local*

dev1-start:
	RIAK_RING_SIZE=$(DEVRINGSIZE) INSTANCE_NAME=grb_local1 RIAK_HANDOFF_PORT=8199 TCP_LIST_PORT=7891 INTER_DC_PORT=8991 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) daemon

dev2-start:
	RIAK_RING_SIZE=$(DEVRINGSIZE) INSTANCE_NAME=grb_local2 RIAK_HANDOFF_PORT=8299 TCP_LIST_PORT=7892 INTER_DC_PORT=8992 $(BASEDIR)/$(DEV2RELPATH)/bin/$(ENVFILE) daemon

dev3-start:
	RIAK_RING_SIZE=$(DEVRINGSIZE) INSTANCE_NAME=grb_local3 RIAK_HANDOFF_PORT=8399 TCP_LIST_PORT=7893 INTER_DC_PORT=8993 $(BASEDIR)/$(DEV3RELPATH)/bin/$(ENVFILE) daemon

dev4-start:
	RIAK_RING_SIZE=$(DEVRINGSIZE) INSTANCE_NAME=grb_local4 RIAK_HANDOFF_PORT=8499 TCP_LIST_PORT=7894 INTER_DC_PORT=8994 $(BASEDIR)/$(DEV4RELPATH)/bin/$(ENVFILE) daemon

devstart: clean devrelclean devrel dev1-start dev2-start dev3-start dev4-start

dev2-join:
	INSTANCE_NAME=grb_local2 $(BASEDIR)/$(DEV2RELPATH)/bin/$(ENVFILE) eval 'riak_core:join("grb_local1@127.0.0.1").'

dev3-join:
	INSTANCE_NAME=grb_local3 $(BASEDIR)/$(DEV3RELPATH)/bin/$(ENVFILE) eval 'riak_core:join("grb_local1@127.0.0.1").'

dev4-join:
	INSTANCE_NAME=grb_local4 $(BASEDIR)/$(DEV4RELPATH)/bin/$(ENVFILE) eval 'riak_core:join("grb_local1@127.0.0.1").'

devjoin: dev2-join dev3-join dev4-join

dev-cluster-plan:
	INSTANCE_NAME=grb_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) eval 'riak_core_claimant:plan().'

dev-cluster-commit:
	INSTANCE_NAME=grb_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) eval 'riak_core_claimant:commit().'

dev-status:
	INSTANCE_NAME=grb_local1 $(BASEDIR)/$(DEV1RELPATH)/bin/$(ENVFILE) eval 'riak_core_console:member_status([]).'

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

devfulljoin:
	./bin/join_cluster_script.erl 'grb_local1@127.0.0.1' 'grb_local2@127.0.0.1'
	./bin/join_cluster_script.erl 'grb_local3@127.0.0.1' 'grb_local4@127.0.0.1'
	./bin/connect_dcs.erl 'grb_local1@127.0.0.1' 'grb_local3@127.0.0.1'

devreplicas:
	./bin/join_cluster_script.erl 'grb_local1@127.0.0.1'
	./bin/join_cluster_script.erl 'grb_local2@127.0.0.1'
	./bin/join_cluster_script.erl 'grb_local3@127.0.0.1'
	./bin/join_cluster_script.erl 'grb_local4@127.0.0.1'
	./bin/connect_dcs.erl 'grb_local1@127.0.0.1' 'grb_local2@127.0.0.1' 'grb_local3@127.0.0.1' 'grb_local4@127.0.0.1'
