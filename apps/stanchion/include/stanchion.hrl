%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%               2021 TI Tokyo    All Rights Reserved.
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

-record(context, {auth_bypass :: boolean(),
                  bucket :: undefined | binary(),
                  owner_id :: undefined | all | string()}).

-define(TURNAROUND_TIME(Call),
        begin
            StartTime_____tat = os:timestamp(),
            Result_____tat  = (Call),
            EndTime_____tat = os:timestamp(),
            {Result_____tat,
             timer:now_diff(EndTime_____tat,
                            StartTime_____tat)}
        end).
