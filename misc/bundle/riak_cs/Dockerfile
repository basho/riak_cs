ARG RIAK_CS_VSN=3.0.0

FROM erlang:22.3.4.10 AS compile-image
ARG RIAK_CS_VSN

WORKDIR /usr/src
ADD https://github.com/TI-Tokyo/riak_cs/archive/refs/tags/${RIAK_CS_VSN}.tar.gz source.tar.gz
RUN tar xzf source.tar.gz
RUN mv riak_cs-${RIAK_CS_VSN} rcs
WORKDIR rcs

EXPOSE 8080 8000 8087

RUN ./rebar3 as rel release

FROM debian:buster AS runtime-image

RUN apt-get update && apt-get -y install libssl1.1 #curl

COPY --from=compile-image /usr/src/rcs/_build/rel/rel/riak-cs /opt/riak-cs

RUN echo "anonymous_user_creation = on" >>/opt/riak-cs/etc/riak-cs.conf

## whatever works
#RUN echo "$riak_host:$riak_http_port/ping" >/tmp/curlop
#CMD while ! curl -s $(cat /tmp/curlop) >/dev/null; do sleep 1; done && /opt/riak-cs/bin/riak-cs foreground

# We can't start riak-cs it in CMD because at this moment as we don't
# yet know riak's addresses -- those are to be allocated by docker
# stack and need to be discovered after that.  All we can do is
# prepare the container, for a script run after docker stack deploy to
# do orchestration aid in the form of sed'ding the right values into
# riak-cs.conf.  It is unfortunate we have to plug a sleep loop for
# the process being monitored by docker, but that's one practical
# solution I have in mind now.

CMD while :; do sleep 1m; done
