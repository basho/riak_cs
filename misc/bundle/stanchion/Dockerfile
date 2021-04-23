FROM erlang:22.3.4.10 AS compile-image

ARG riak_host=172.18.0.3 \
    riak_pb_port=8087 \
    stanchion_host=172.18.0.2 \
    stanchion_port=8085

ENV tag="3.0.0pre1"

EXPOSE $stanchion_port

WORKDIR /usr/src/stanchion
ADD https://github.com/TI-Tokyo/stanchion/archive/refs/tags/$tag.tar.gz source.tar.gz
RUN tar xzf source.tar.gz
WORKDIR stanchion-$tag

RUN sed -i \
    -e "s/@stanchion_ip@/$stanchion_host/" \
    -e "s/@stanchion_port@/$stanchion_port/" \
    -e "s/@riak_ip@/$riak_host/" \
    -e "s/@riak_pb_port@/$riak_pb_port/" \
    rel/docker/vars.config

RUN ./rebar3 as docker release
RUN mv _build/docker/rel/stanchion ../rel
RUN mv rel/docker/tini ..

FROM debian:buster AS runtime-image

RUN apt-get update && apt-get -y install libssl1.1

COPY --from=compile-image /usr/src/stanchion/rel /opt/stanchion

RUN mkdir -p /etc/stanchion /var/lib/stanchion /var/lib/stanchion/data
RUN mv /opt/stanchion/etc/stanchion.conf /etc/stanchion

COPY --from=compile-image /usr/src/stanchion/tini /tini
RUN chmod +x /tini

ENTRYPOINT ["/tini", "--"]
CMD ["/opt/stanchion/bin/stanchion", "foreground"]
