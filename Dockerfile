FROM erlang:22.3.4.10 AS compile-image

ARG stanchion_host=127.0.0.1 \
    stanchion_port=8085 \
    riak_host=127.0.0.1 \
    riak_pb_port=8087 \
    cs_host=127.0.0.1 \
    cs_port=8080 \
    admin_host=127.0.0.1 \
    admin_port=8000

EXPOSE $cs_port

WORKDIR /usr/src/riak_cs
COPY . /usr/src/riak_cs

RUN sed -i \
    -e "s/@cs_ip@/$cs_host/" \
    -e "s/@cs_port@/$cs_port/" \
    -e "s/@admin_ip@/$admin_host/" \
    -e "s/@admin_port@/$admin_port/" \
    -e "s/@stanchion_ip@/$stanchion_host/" \
    -e "s/@stanchion_port@/$stanchion_port/" \
    -e "s/@riak_ip@/$riak_host/" \
    -e "s/@riak_pb_port@/$riak_pb_port/" \
    rel/docker/vars.config
RUN ./rebar3 as docker release

FROM debian:buster AS runtime-image

RUN apt-get update && apt-get -y install libssl1.1

COPY --from=compile-image /usr/src/riak_cs/_build/docker/rel/riak-cs /opt/riak-cs

RUN mkdir -p /etc/riak-cs /var/lib/riak-cs /var/lib/riak-cs/data
RUN mv /opt/riak-cs/etc/riak-cs.conf /etc/riak-cs

COPY --from=compile-image /usr/src/riak_cs/rel/docker/tini /tini
RUN chmod +x /tini

ENTRYPOINT ["/tini", "--"]
CMD ["/opt/riak-cs/bin/riak-cs", "foreground"]
