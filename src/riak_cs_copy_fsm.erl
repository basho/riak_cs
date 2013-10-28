%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_copy_fsm).

%% check_0length_metadata_update(Length, RD, Ctx=#context{local_context=LocalCtx}) ->
%%     %% The authorize() callback has already been called, which means
%%     %% that ensure_doc() has been called, so the local context
%%     %% manifest is up-to-date: the object exists or it doesn't.
%%     case (not is_atom(LocalCtx#key_context.manifest) andalso
%%           zero_length_metadata_update_p(Length, RD)) of
%%         false ->
%%             UpdLocalCtx = LocalCtx#key_context{size=Length},
%%             {true, RD, Ctx#context{local_context=UpdLocalCtx}};
%%         true ->
%%             UpdLocalCtx = LocalCtx#key_context{size=Length,
%%                                                update_metadata=true},
%%             {true, RD, Ctx#context{local_context=UpdLocalCtx}}
%%     end.

%% zero_length_metadata_update_p(0, RD) ->
%%     OrigPath = wrq:get_req_header("x-rcs-rewrite-path", RD),
%%     case wrq:get_req_header("x-amz-copy-source", RD) of
%%         undefined ->
%%             false;
%%         [$/ | _] = Path ->
%%             Path == OrigPath;
%%         Path ->
%%             %% boto (version 2.7.0) does NOT prepend "/"
%%             [$/ | Path] == OrigPath
%%     end;
%% zero_length_metadata_update_p(_, _) ->
%%     false.
