%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved,
%%               2021-2023 TI Tokyo    All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

%% @doc Behaviour for Quota / QoS hook point, to save CS systems by
%% limiting loads. The idea of Quota and QoS system is, to limit
%% things by pluggable modules. This is because requirements vastly
%% varies and it is too hard to cover all of them with single
%% implementation.
%%
%% This is experimental feature. These interfaces and behaviour may or
%% may not change until general availability.
%%
%% To use 'inherited' your own module, it should be configured in
%% application environment (set it in advanced.config). The item name
%% is `quota_modules'. Example:
%%
%% {riak_cs, [
%%   {quota_modules, [riak_cs_simple_quota, riak_cs_bwlimiter]},
%%   ...
%% ]},
%%
%% The order of module names is also the order of evaluation. Modules
%% are evaluated every time 1) incoming requests passed every other
%% authentication, authorization works (including ACL, Policy), and 2)
%% all incoming requests successfully finished even if those requests
%% resulted in 50x or whatever.
%%
%% 1) is a callback called `allow/3' and 2) is `update/2` callback.
%% These callbacks are called in each process context, 1) is at
%% mochiweb acceptor and 2) is webmachine log handler. Another
%% callback `start_link/0' is to let supervisor know the quota module
%% needs a state management server or not. These processes can't be
%% added nor removed at runtime, please just control such
%% enabled/disabled status inside plugin modules.
%%
%% Return value of `allow/3' is evaluated at `error_response/3', where
%% client return behaviour can be controlled here, such as http status
%% codes, headers and body.
%%
%% Known limitations:
%% - currently this is only for objects.
%% - separate owner and accessor case: bandwidth and access should be
%%   for accessor, while owner is for usage
%% - no integration tests, please report bugs if you find any!
%% - No cool configuration systems
%% - No cool operation console: use `riak-cs attach' and send msg to processes
%% - Multibag

-module(riak_cs_quota).

-export([invoke_all_callbacks/3,
         update_all_states/2,
         handle_error/4]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine_logger.hrl").
-include_lib("webmachine/include/wm_reqdata.hrl").
-include_lib("kernel/include/logger.hrl").

%% Callbacks as a behaviour

%% @doc initalize a global state of a quota instance. This state is
%% preserved while this quota process is alive. As no system
%% configurations provided, module programmers should set their own
%% configuration system. So aren't command-line console.
-callback start_link() ->
    {ok, pid()} | ignore | {error, Reason :: term()}.

%% @doc note that this callback is not called at single process, but
%% called at each mochiweb acceptor context.
-callback allow(
            %% Owner of the target object
            Owner :: rcs_user()  %% Typical access, bucket owner
                     | undefined %% Anonymous access
                     | string()|binary(),  %% Other authenticated user
            Access :: access(), %% including accessor, bucket, key, RD
            Context::term()) ->
    {ok, Request::#wm_reqdata{}, Context::#rcs_s3_context{}} |
    {error, Reason::term()}.

-callback error_response(Reason :: term(),
                         #wm_reqdata{}, #rcs_s3_context{}) ->
    {non_neg_integer() | {non_neg_integer(), string()},
     #wm_reqdata{}, #rcs_s3_context{}}.

%% @doc for now, to know the accessor, RD should be retrieved from
%% headers in LogData (not knowing whether he's authenticated or not)
-callback update(User :: binary(), %% Bucket owner ... Should be changed to accessor?
                 WMLogData :: wm_log_data()) ->
    ok | {error, term()}.


%% Invoke things; below are not for plugin writers

invoke_all_callbacks(Owner, Access, Ctx0) ->
    Modules = riak_cs_config:quota_modules(),
    lists:foldl(fun(Module, {ok, RD, Ctx}) ->
                        case Module:allow(Owner,
                                          Access#access_v1{req=RD},
                                          Ctx) of
                            {ok, _, _} = R->
                                R;
                            {error, Reason} ->
                                logger:info("quota check failed at ~p: ~p", [Module, Reason]),
                                {error, Module, Reason, RD, Ctx}
                        end;
                   (_, Other) ->
                        Other
                end, {ok, Access#access_v1.req, Ctx0}, Modules).

-spec update_all_states(iolist(), #wm_log_data{}) -> no_return().
update_all_states(User, LogData) ->
    Modules = riak_cs_config:quota_modules(),
    [begin
         ?LOG_DEBUG("quota update at ~p: ~p", [Module, User]),
         (catch Module:update(list_to_binary(User), LogData))
     end || Module <- Modules].

handle_error(Module, Reason, RD0, Ctx0) ->
    {Code, RD, Ctx} = Module:error_response(Reason, RD0, Ctx0),
    {{halt, Code}, RD, Ctx}.
