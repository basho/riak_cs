ARG RIAK_CS_CONTROL_VSN=3.0.0 \
    CS_HOST="172.18.0.4" \
    CS_PORT=8080 \
    CS_PROTO="http" \
    CS_CONTROL_PORT=8090 \
    CS_ADMIN_KEY="admin-key" \
    CS_ADMIN_SECRET="admin-secret"

FROM erlang:22.3.4.10 AS compile-image
ARG RIAK_CS_CONTROL_VSN

EXPOSE $cs_control_port

WORKDIR /usr/src
ADD https://github.com/TI-Tokyo/riak_cs_control/archive/refs/tags/${RIAK_CS_CONTROL_VSN}.tar.gz source.tar.gz
RUN tar xzf source.tar.gz
RUN mv riak_cs_control-${RIAK_CS_CONTROL_VSN} rcc
WORKDIR rcc

RUN ./rebar3 as rel release

FROM debian:buster AS runtime-image
ARG CS_HOST \
    CS_PORT \
    CS_PROTO \
    CS_CONTROL_PORT \
    CS_ADMIN_KEY \
    CS_ADMIN_SECRET

ENV CS_HOST=${CS_HOST:-127.0.0.1} \
    CS_PORT=${CS_PORT:-8080} \
    CS_PROTO=${CS_PROTO:-http} \
    CS_CONTROL_PORT=${CS_CONTROL_PORT:-8090} \
    CS_ADMIN_KEY=${CS_ADMIN_KEY:-"admin-key"} \
    CS_ADMIN_SECRET=${CS_ADMIN_SECRET:-"admin-secret"}

RUN apt-get update && apt-get -y install libssl1.1 curl

COPY --from=compile-image /usr/src/rcc/_build/rel/rel/riak_cs_control /opt/riak_cs_control

CMD /opt/riak_cs_control/bin/riak_cs_control foreground
