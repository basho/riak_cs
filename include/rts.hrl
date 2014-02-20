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
         'BytesOutIncomplete', 'Objects', 'Bytes']).

-define(SUPPORTED_USAGE_FIELD_BIN,
        [<<"Count">> , <<"UserErrorCount">> , <<"SystemErrorCount">>,
         <<"BytesIn">> , <<"UserErrorBytesIn">> , <<"SystemErrorBytesIn">>,
         <<"BytesOut">> , <<"UserErrorBytesOut">> , <<"SystemErrorBytesOut">>,
         <<"BytesOutIncomplete">>, <<"Objects">>, <<"Bytes">>]).
