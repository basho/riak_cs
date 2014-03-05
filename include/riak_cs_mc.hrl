%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

%% Riak's bucket and key to store usage information
-define(USAGE_BUCKET, <<"riak-cs-mc">>).
-define(USAGE_KEY,    <<"usage">>).

-record(usage, {
          bag_id :: riak_cs_mc:bag_id(),
          weight :: non_neg_integer(),
          free :: non_neg_integer(),     % not used currently
          total :: non_neg_integer()     % not used currently
          }).
