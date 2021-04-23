FROM centos:latest

ARG RIAK_VERSION=3.0.4

LABEL Description="Riak" \
      "Riak Version"="$RIAK_VERSION"

RUN set -euxo pipefail && \
    yum install -y "http://s3.amazonaws.com/downloads.basho.com/riak/$(sed 's/.[[:digit:]]*$//' <<< "$RIAK_VERSION")/$RIAK_VERSION/rhel/6/riak-$RIAK_VERSION-1.el6.x86_64.rpm" && \
    yum install -y java && \
    yum autoremove -y && \
    yum clean all && \
    rm -rf /var/cache/yum

COPY conf/riak.conf /etc/riak/

EXPOSE 8087 8098

RUN chown riak:riak /var/lib/riak

#USER riak
CMD chown -R riak:riak /var/lib/riak && \
    su riak -c \
      "riak start && sleep 3 && \
       cat /var/log/riak/*; \
       tail -f /dev/null /var/log/riak/*"
