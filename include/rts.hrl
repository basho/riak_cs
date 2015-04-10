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

%% JSON keys used by rts module
-define(START_TIME, <<"StartTime">>).
-define(END_TIME, <<"EndTime">>).

%% http://docs.basho.com/riakcs/latest/cookbooks/Querying-Access-Statistics/
-type usage_field_type() :: 'Count' | 'UserErrorCount' | 'SystemErrorCount'
                          | 'BytesIn' | 'UserErrorBytesIn' | 'SystemErrorBytesIn'
                          | 'BytesOut' | 'UserErrorBytesOut' | 'SystemErrorBytesOut'
                          | 'BytesOutIncomplete' | 'Objects'| 'Bytes'.

-define(SUPPORTED_USAGE_FIELD,
        ['Count' , 'UserErrorCount' , 'SystemErrorCount',
         'BytesIn' , 'UserErrorBytesIn' , 'SystemErrorBytesIn',
         'BytesOut' , 'UserErrorBytesOut' , 'SystemErrorBytesOut',
         'BytesOutIncomplete',
         'wr_old-ct', 'wr_old-by', 'wr_old-bl',
         'wr_new-ct', 'wr_new-by', 'wr_new-bl',
         'wr_mp-ct', 'wr_mp-by', 'wr_mp-bl',
         'Objects', 'Bytes', 'user-bl',
         'sd_old-ct', 'sd_old-by', 'sd_old-bl',
         'sd_new-ct', 'sd_new-by', 'sd_new-bl',
         'pd_old-ct','pd_old-by', 'pd_old-bl',
         'pd_new-ct','pd_new-by', 'pd_new-bl',
         'ac_de-ct','ac_de-by', 'ac_de-bl']).

-define(SUPPORTED_USAGE_FIELD_BIN,
        [<<"Count">> , <<"UserErrorCount">> , <<"SystemErrorCount">>,
         <<"BytesIn">> , <<"UserErrorBytesIn">> , <<"SystemErrorBytesIn">>,
         <<"BytesOut">> , <<"UserErrorBytesOut">> , <<"SystemErrorBytesOut">>,
         <<"BytesOutIncomplete">>,
         <<"wr_old-ct">>, <<"wr_old-by">>, <<"wr_old-bl">>,
         <<"wr_new-ct">>, <<"wr_new-by">>, <<"wr_new-bl">>,
         <<"wr_mp-ct">>, <<"wr_mp-by">>, <<"wr_mp-bl">>,
         <<"Objects">>, <<"Bytes">>, <<"user-bl">>,
         <<"sd_old-ct">>, <<"sd_old-by">>, <<"sd_old-bl">>,
         <<"sd_new-ct">>, <<"sd_new-by">>, <<"sd_new-bl">>,
         <<"pd_old-ct">>,<<"pd_old-by">>, <<"pd_old-bl">>,
         <<"pd_new-ct">>,<<"pd_new-by">>, <<"pd_new-bl">>,
         <<"ac_de-ct">>,<<"ac_de-by">>, <<"ac_de-bl">>]).
