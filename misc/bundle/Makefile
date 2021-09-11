.PHONY: ensure-dirs up down clean

RIAK_VSN       	    ?= 3.0.7
RIAK_CS_VSN    	    ?= 3.0.0
STANCHION_VSN  	    ?= 3.0.0
RIAK_CS_CONTROL_VSN ?= 3.0.0

RIAK_DATA      ?= $(shell pwd)/data/riak
RIAK_CS_DATA   ?= $(shell pwd)/data/riak_cs
STANCHION_DATA ?= $(shell pwd)/data/stanchion

build:
	@docker-compose build \
	    --build-arg RIAK_VSN=$(RIAK_VSN) \
	    --build-arg RIAK_CS_VSN=$(RIAK_CS_VSN) \
	    --build-arg STANCHION_VSN=$(STANCHION_VSN) \
	    --build-arg RIAK_CS_CONTROL_VSN=$(RIAK_CS_CONTROL_VSN)

up: ensure-dirs build
	@RIAK_DATA=$(RIAK_DATA) RIAK_CS_DATA=$(RIAK_CS_DATA) STANCHION_DATA=$(STANCHION_DATA) \
	    docker-compose up --detach

dev-up: ensure-dirs build
	@RIAK_DATA=$(RIAK_DATA) RIAK_CS_DATA=$(RIAK_CS_DATA) STANCHION_DATA=$(STANCHION_DATA) \
	    docker-compose up

down:
	@RIAK_DATA=$(RIAK_DATA) RIAK_CS_DATA=$(RIAK_CS_DATA) STANCHION_DATA=$(STANCHION_DATA) \
	    docker-compose down

ensure-dirs:
	@mkdir -p $(RIAK_DATA) $(RIAK_CS_DATA) $(STANCHION_DATA)

clean:
	@rm -rf $(RIAK_DATA) $(RIAK_CS_DATA) $(STANCHION_DATA)
