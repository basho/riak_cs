# This is a draft Dockerfile for riak_cs
# NOT TESTED! Final version will be re-uploaded when ready.

FROM erlang:22.3.4.10 AS compile-image

WORKDIR /usr/src/riak_cs
COPY . /usr/src/riak_cs

RUN rebar3 as prod tar

RUN mkdir -p /opt/riak_cs
RUN tar -zxvf /usr/src/riak_cs/_build/prod/rel/*/*.tar.gz -C /opt/riak_cs

FROM debian:buster AS runtime-image

# the usual dance with libssl and crypto (depending on what's shipped
# with this debian version):
#RUN apt-get update && apt-get -y install libssl1.1

COPY --from=compile-image /opt/riak_cs /opt/riak_cs

# Add Tini -- this is meant to solve the PID 1 zombie reaping problem
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

ENTRYPOINT ["/tini", "--"]
CMD ["/opt/riak_cs/bin/riak_cs", "foreground"]
