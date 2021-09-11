ARG STANCHION_VSN=3.0.0 \
    riak_host=172.18.0.2 \
    riak_pb_port=8087 \
    riak_http_port=8098 \
    stanchion_host=172.18.0.3 \
    stanchion_port=8085

FROM erlang:22.3.4.10 AS compile-image
ARG STANCHION_VSN \
    riak_host \
    riak_pb_port \
    riak_http_port \
    stanchion_host \
    stanchion_port

EXPOSE $stanchion_port

WORKDIR /usr/src
ADD https://github.com/TI-Tokyo/stanchion/archive/refs/tags/${STANCHION_VSN}.tar.gz source.tar.gz
RUN tar xzf source.tar.gz
RUN mv stanchion-${STANCHION_VSN} s
WORKDIR s

RUN sed -i \
    -e "s/@stanchion_ip@/$stanchion_host/" \
    -e "s/@stanchion_port@/$stanchion_port/" \
    -e "s/@riak_ip@/$riak_host/" \
    -e "s/@riak_pb_port@/$riak_pb_port/" \
    rel/docker/vars.config

RUN ./rebar3 as docker release


FROM debian:buster AS runtime-image
ARG riak_host \
    riak_http_port

RUN apt-get update && apt-get -y install libssl1.1 curl

COPY --from=compile-image /usr/src/s/_build/docker/rel/stanchion /opt/stanchion

RUN mkdir -p /etc/stanchion /var/lib/stanchion /var/lib/stanchion/data
RUN mv /opt/stanchion/etc/stanchion.conf /etc/stanchion

# whatever works
RUN echo "$riak_host:$riak_http_port/ping" >/tmp/curlop
CMD while ! curl -s $(cat /tmp/curlop) >/dev/null; do sleep 1; done && /opt/stanchion/bin/stanchion foreground
