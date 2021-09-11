ARG RIAK_CS_VSN=3.0.0 \
    cs_host=172.18.0.4 \
    cs_port=8080 \
    admin_host=172.18.0.4 \
    admin_port=8000 \
    stanchion_host=172.18.0.3 \
    stanchion_port=8085 \
    riak_host=172.18.0.2 \
    riak_http_port=8098 \
    riak_pb_port=8087

FROM erlang:22.3.4.10 AS compile-image
ARG RIAK_CS_VSN \
    cs_host \
    cs_port \
    admin_host \
    admin_port \
    stanchion_host \
    stanchion_port \
    riak_host \
    riak_http_port \
    riak_pb_port

WORKDIR /usr/src
ADD https://github.com/TI-Tokyo/riak_cs/archive/refs/tags/${RIAK_CS_VSN}.tar.gz source.tar.gz
RUN tar xzf source.tar.gz
RUN mv riak_cs-${RIAK_CS_VSN} rcs
WORKDIR rcs

EXPOSE $cs_port $admin_port

RUN sed \
    -e "s/@cs_ip@/$cs_host/" \
    -e "s/@cs_port@/$cs_port/" \
    -e "s/@admin_ip@/$admin_host/" \
    -e "s/@admin_port@/$admin_port/" \
    -e "s/@stanchion_ip@/$stanchion_host/" \
    -e "s/@stanchion_port@/$stanchion_port/" \
    -e "s/@riak_ip@/$riak_host/" \
    -e "s/@riak_pb_port@/$riak_pb_port/" \
    rel/docker/vars.config.src >rel/docker/vars.config

RUN ./rebar3 as docker release


FROM debian:buster AS runtime-image
ARG cs_host \
    cs_port \
    admin_host \
    admin_port \
    stanchion_host \
    stanchion_port \
    riak_host \
    riak_http_port \
    riak_pb_port

RUN apt-get update && apt-get -y install libssl1.1 curl

COPY --from=compile-image /usr/src/rcs/_build/docker/rel/riak-cs /opt/riak-cs

RUN mkdir -p /etc/riak-cs /var/lib/riak_cs /var/lib/riak_cs/data
RUN mv /opt/riak-cs/etc/riak-cs.conf /etc/riak-cs

RUN echo "anonymous_user_creation = on" >>/etc/riak-cs/riak-cs.conf

# whatever works
RUN echo "$riak_host:$riak_http_port/ping" >/tmp/curlop
CMD while ! curl -s $(cat /tmp/curlop) >/dev/null; do sleep 1; done && /opt/riak-cs/bin/riak-cs foreground
