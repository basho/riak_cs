ARG STANCHION_VSN=3.0.0

FROM erlang:22.3.4.10 AS compile-image
ARG STANCHION_VSN

EXPOSE 8085

WORKDIR /usr/src
ADD https://github.com/TI-Tokyo/stanchion/archive/refs/tags/${STANCHION_VSN}.tar.gz source.tar.gz
RUN tar xzf source.tar.gz
RUN mv stanchion-${STANCHION_VSN} s
WORKDIR s

RUN ./rebar3 as rel release


FROM debian:buster AS runtime-image

RUN apt-get update && apt-get -y install libssl1.1 #curl

COPY --from=compile-image /usr/src/s/_build/rel/rel/stanchion /opt/stanchion

## whatever works
#RUN echo "$riak_host:$riak_http_port/ping" >/tmp/curlop
#CMD while ! curl -s $(cat /tmp/curlop) >/dev/null; do sleep 1; done && /opt/stanchion/bin/stanchion foreground

# We can't start riak-cs it in CMD because at this moment as we don't
# yet know riak's addresses -- those are to be allocated by docker
# stack and need to be discovered after that.  All we can do is
# prepare the container, for a script run after docker stack deploy to
# do orchestration aid in the form of sed'ding the right values into
# riak-cs.conf.  It is unfortunate we have to plug a sleep loop for
# the process being monitored by docker, but that's one practical
# solution I have in mind now.

CMD while :; do sleep 1m; done
