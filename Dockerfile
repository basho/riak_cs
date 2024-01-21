# To override the default Erlang version used in the build, use:
# $ docker build --build-arg erlang_version=22 ...

ARG erlang_version="25"
FROM erlang:${erlang_version} AS compile-image

EXPOSE 8080 8085

WORKDIR /usr/src/riak_cs
COPY . /usr/src/riak_cs

# When running in a docker container, ideally we would want our app to
# be configurable via environment variables (option --env-file to
# docker run).  For that reason, We use a pared-down, cuttlefish-less
# rebar.config.  Configuration from environment now becomes possible,
# via rebar's own method of generating sys.config from
# /sys.config.src.
RUN make rel-docker

FROM debian:latest AS runtime-image

COPY --from=compile-image /usr/src/riak_cs/rel/riak-cs /opt/riak-cs

ENTRYPOINT [ "/opt/riak-cs/bin/riak-cs" "foreground" ]
