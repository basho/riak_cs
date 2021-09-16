.PHONY: ensure-dirs build-single build-scalable up-single up-scalable down-single down scalable clean

RIAK_VSN       	    ?= 3.0.7
RIAK_CS_VSN    	    ?= 3.0.0pre8
STANCHION_VSN  	    ?= 3.0.0pre8
RIAK_CS_CONTROL_VSN ?= 3.0.0pre3

RIAK_DATA      ?= $(shell pwd)/data/riak
RIAK_CS_DATA   ?= $(shell pwd)/data/riak_cs
STANCHION_DATA ?= $(shell pwd)/data/stanchion

N_RIAK_NODES     ?= 3
N_RCS_NODES      ?= 2
RCS_AUTH_V4      ?= on

DOCKER_SERVICE_NAME ?= rcs-tussle-one

build-single:
	@COMPOSE_FILE=docker-compose-single.yml \
	docker-compose build \
	    --build-arg RIAK_VSN=$(RIAK_VSN) \
	    --build-arg RIAK_CS_VSN=$(RIAK_CS_VSN) \
	    --build-arg STANCHION_VSN=$(STANCHION_VSN) \
	    --build-arg RIAK_CS_CONTROL_VSN=$(RIAK_CS_CONTROL_VSN)
build-scalable:
	@COMPOSE_FILE=docker-compose-scalable-build.yml \
	docker-compose build \
	    --build-arg RIAK_VSN=$(RIAK_VSN) \
	    --build-arg RIAK_CS_VSN=$(RIAK_CS_VSN) \
	    --build-arg STANCHION_VSN=$(STANCHION_VSN) \
	    --build-arg RIAK_CS_CONTROL_VSN=$(RIAK_CS_CONTROL_VSN)

up-single: ensure-dirs build-single
	@COMPOSE_FILE=docker-compose-single.yml \
	 RIAK_DATA=$(RIAK_DATA) \
	 RIAK_CS_DATA=$(RIAK_CS_DATA) \
	 STANCHION_DATA=$(STANCHION_DATA) \
	    docker-compose up --detach

up-scalable: build-scalable
	@docker swarm init >/dev/null || :
	@COMPOSE_FILE=docker-compose-scalable-run.yml \
	 N_RIAK_NODES=$(N_RIAK_NODES) \
	 N_RCS_NODES=$(N_RCS_NODES) \
	    docker stack deploy -c docker-compose-scalable-run.yml $(DOCKER_SERVICE_NAME) \
	 && ./make_riak_cluster.py \
		$(DOCKER_SERVICE_NAME) \
		$(N_RIAK_NODES) \
		$(N_RCS_NODES) \
		$(RCS_AUTH_V4)

down-single:
	@COMPOSE_FILE=docker-compose-single.yml \
	    docker-compose down

down-scalable:
	@COMPOSE_FILE=docker-compose-scalable-run.yml \
	    docker stack rm $(DOCKER_SERVICE_NAME)

ensure-dirs:
	@mkdir -p $(RIAK_DATA) $(RIAK_CS_DATA) $(STANCHION_DATA)

clean:
	@rm -rf $(RIAK_DATA) $(RIAK_CS_DATA) $(STANCHION_DATA)
