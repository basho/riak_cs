FROM erlang:22.3.4.10 AS compile-image

ARG STANCHION_VSN=3.0.0 \
    riak_host=172.18.0.3 \
    riak_pb_port=8087 \
    riak_http_port=8098 \
    stanchion_host=172.18.0.2 \
    stanchion_port=8085

EXPOSE $stanchion_port

WORKDIR /usr/src/stanchion
ADD https://github.com/TI-Tokyo/stanchion/archive/refs/tags/${STANCHION_VSN}.tar.gz source.tar.gz
RUN tar xzf source.tar.gz
WORKDIR stanchion-${STANCHION_VSN}

RUN sed -i \
    -e "s/@stanchion_ip@/$stanchion_host/" \
    -e "s/@stanchion_port@/$stanchion_port/" \
    -e "s/@riak_ip@/$riak_host/" \
    -e "s/@riak_pb_port@/$riak_pb_port/" \
    rel/docker/vars.config

RUN ./rebar3 as docker release
RUN mv _build/docker/rel/stanchion ../rel

# relying on depends_on: in the docker-copose.yml is not sufficient,
# so let's ensure riak is actually up and responding before starting
# ourselves (also, variables don't seem to survive the FROM barrier):
RUN echo "riak:${riak_http_port}/ping" >/tmp/riak_host


FROM debian:buster AS runtime-image

RUN apt-get update && apt-get -y install libssl1.1 curl

COPY --from=compile-image /usr/src/stanchion/rel /opt/stanchion
COPY --from=compile-image /tmp/riak_host /tmp/riak_host

RUN mkdir -p /etc/stanchion /var/lib/stanchion /var/lib/stanchion/data
RUN mv /opt/stanchion/etc/stanchion.conf /etc/stanchion

CMD while ! curl -s $(cat /tmp/riak_host); do sleep 1; done && /opt/stanchion/bin/stanchion foreground
