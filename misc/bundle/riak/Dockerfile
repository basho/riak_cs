FROM debian:buster

ARG RIAK_VSN=3.0.7

ENV PKG=riak_${RIAK_VSN}-OTP22.3_amd64.deb

RUN apt-get update && apt-get install -y wget logrotate sudo
RUN wget -q https://files.tiot.jp/riak/kv/3.0/${RIAK_VSN}/debian/10/${PKG}
RUN dpkg -i ${PKG} && rm ${PKG}

RUN sed -i \
    -e "s|storage_backend = bitcask|storage_backend = multi|" \
    /etc/riak/riak.conf
RUN echo "buckets.default.allow_mult = true" >>/etc/riak/riak.conf
RUN echo "buckets.default.merge_strategy = 2" >>/etc/riak/riak.conf

RUN sed -i \
    -e "s|]\\.|, \
    {riak_kv, [ \
      {multi_backend, \
          [{be_default,riak_kv_eleveldb_backend, \
               [{max_open_files,20}]}, \
           {be_blocks,riak_kv_bitcask_backend, \
               []}]}, \
      {multi_backend_default,be_default}, \
      {multi_backend_prefix_list,[{<<\"0b:\">>,be_blocks}]}, \
      {storage_backend,riak_kv_multi_backend} \
     ]} \
     ].|" \
     /etc/riak/advanced.config

EXPOSE 8087 8098 9080

RUN chown -R riak:riak /var/lib/riak
RUN echo "riak soft nofile 65536" >>/etc/security/limits.conf
RUN echo "riak hard nofile 65536" >>/etc/security/limits.conf

#USER riak

# We can't start riak it in CMD because at this moment as we don't yet
# know other riak nodes' addresses -- those are to be allocated by
# docker stack and need to be discovered after that.  All we can do is
# prepare the container, for a script run after docker stack deploy to
# do orchestration aid in the form of sed'ding the right values into
# riak.conf.  It is unfortunate we have to plug a sleep loop for the
# process being monitored by docker, but that's one practical solution
# I have in mind now.

CMD while :; do sleep 1m; done
