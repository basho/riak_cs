-module(rtcs_object_reader).

-compile(export_all).

start_link(Bucket, Key, UserConfig) ->
    Pid = spawn_link(fun() ->
                             object_reader(Bucket, Key, UserConfig, 1)
                     end),
    {ok, Pid}.

stop(Pid) ->
    Pid ! {stop, self()},
    receive Reply -> Reply end.

object_reader(Bucket, Key, UserConfig, IntervalSec)->
    erlcloud_s3:get_object(Bucket, Key, UserConfig),
    receive
        {stop, From} -> From ! ok
    after IntervalSec * 1000 ->
            object_reader(Bucket, Key, UserConfig, IntervalSec)
    end.
