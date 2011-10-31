-module(riak_moss_wm_error_handler).
-export([render_error/3]).

render_error(_Code, Req, _Reason) ->
    Req:response_body().


