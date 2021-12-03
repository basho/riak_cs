#!/bin/sh

# $FreeBSD$
#
# PROVIDE: riak_cs
# REQUIRE: LOGIN
# KEYWORD: shutdown

. /etc/rc.subr

name=riak_cs
command=/usr/local/lib/riak-cs/%ERTS_PATH%/bin/beam.smp
rcvar=riak_cs_enable
start_cmd="/usr/local/lib/riak-cs/bin/riak-cs start"
stop_cmd="/usr/local/lib/riak-cs/bin/riak-cs stop"
pidfile="/var/run/riak-cs/riak-cs.pid"

load_rc_config $name
run_rc_command "$1"
