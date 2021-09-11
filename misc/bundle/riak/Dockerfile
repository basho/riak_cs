FROM debian:buster

ARG RIAK_VSN=3.0.7 \
    listener_ip=172.18.0.2 \
    listener_pb_port=8087 \
    listener_http_port=8098

ENV PKG=riak_${RIAK_VSN}-OTP22.3_amd64.deb

RUN apt-get update && apt-get install -y wget logrotate sudo
RUN wget -q https://files.tiot.jp/riak/kv/3.0/${RIAK_VSN}/debian/10/${PKG}
RUN dpkg -i ${PKG} && rm ${PKG}

RUN sed -i \
    -e "s|listener.protobuf.internal = 127.0.0.1:8087|listener.protobuf.internal = 0.0.0.0:${listener_pb_port}|" \
    -e "s|listener.http.internal = 127.0.0.1:8098|listener.http.internal = 0.0.0.0:${listener_http_port}|" \
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

EXPOSE $listener_pb_port $listener_http_port

RUN chown -R riak:riak /var/lib/riak
RUN echo "riak soft nofile 65536" >>/etc/security/limits.conf
RUN echo "riak hard nofile 65536" >>/etc/security/limits.conf

USER riak
CMD riak foreground
