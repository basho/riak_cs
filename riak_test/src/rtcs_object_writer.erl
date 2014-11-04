-module(rtcs_object_writer).

-compile(export_all).

start_writers(Bucket, Key, Size, UserConfig, IntervalMilliSec, N) ->
    Results = [start_link(Bucket, Key, Size, UserConfig, IntervalMilliSec)
               || _ <- lists:seq(1, N)],
    [Pid || {ok, Pid} <- Results].

stop_writers(PidList) ->
    [stop(Pid) || Pid <- PidList].

start_link(Bucket, Key, Size, UserConfig, IntervalMilliSec) ->
    timer:sleep(IntervalMilliSec + 1000),
    Pid = spawn_link(fun() ->
                             object_writer(Bucket, Key, Size,
                                           UserConfig, IntervalMilliSec)
                     end),
    {ok, Pid}.

stop(Pid) ->
    Pid ! {stop, self()},
    receive Reply -> Reply end.

object_writer(Bucket, Key, Size, UserConfig, IntervalMilliSec)
  when is_integer(Size) andalso Size > 0 ->
    Binary = crypto:rand_bytes(Size),
    object_writer(Bucket, Key, Binary, UserConfig, IntervalMilliSec);
object_writer(Bucket, Key, Binary, UserConfig, IntervalMilliSec)
  when is_binary(Binary) ->
    try
        erlcloud_s3:put_object(Bucket, Key, Binary, UserConfig)
    catch T:E ->
            lager:debug("put_object failed: ~p:~p", [T, E])
    end,
    receive
        {stop, From} -> From ! ok
    after IntervalMilliSec ->
            object_writer(Bucket, Key, Binary, UserConfig, IntervalMilliSec)
    end.

