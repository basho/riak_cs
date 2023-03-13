%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved,
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

-module(prop_riak_cs_aws_auth).

-include("riak_cs.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-define(QC_OUT(P),
        on_output(fun(Str, Args) ->
                          io:format(user, Str, Args) end, P)).
-define(TESTING_TIME, 20).

auth_v2_proper_test_() ->
    Tests =
        [{timeout, ?TESTING_TIME*2,
          ?_assertEqual(true, proper:quickcheck(?QC_OUT(prop_v2_auth())))
         }],
    [{inparallel, Tests}].


prop_v2_auth() ->
    RootHost = "s3.amazonaws.com",
    ok = application:set_env(riak_cs, cs_root_host, RootHost),
    ?FORALL(Request, gen_request(RootHost),
            begin
                {KeyData, KeySecret, RD} = Request,
                SignedString = riak_cs_aws_auth:calculate_signature_v2(KeySecret, RD),
                CSAuthHeader = ["AWS ", KeyData, $:, list_to_binary(SignedString)],

                ErlCloudAuthHeader = erlcloud_hdr(KeyData, KeySecret, RD),
                CSAuthHeader =:= ErlCloudAuthHeader
            end).

gen_request(RootHost) ->
    ?LET(
       {KeyData, KeySecret, ContentMD5, ContentType, Verb, Version, Filename, CSBucket},
       {riak_cs_gen:non_blank_string(),
        riak_cs_gen:non_blank_string(),
        riak_cs_gen:md5(),
        riak_cs_gen:content_type(),
        riak_cs_gen:http_verb(),
        riak_cs_gen:http_version(),
        riak_cs_gen:file_name(),
        riak_cs_gen:bucket()},
       begin
           OrigPath = mochiweb_util:quote_plus(unicode:characters_to_list(Filename)),
           Host = binary_to_list(CSBucket) ++ "." ++ RootHost,
           Date = httpd_util:rfc1123_date(erlang:localtime()),
           Headers0 =
               mochiweb_headers:make([{"Host", Host},
                                      {"Date", Date},
                                      {"Content-MD5", ContentMD5},
                                      {"Content-Type", ContentType}]),
           {Headers, Path} = riak_cs_s3_rewrite:rewrite(Verb, https, Version, Headers0, OrigPath),
           RD = wrq:create(Verb, Version, Path, Headers),
           {KeyData, KeySecret, RD}
       end).

erlcloud_hdr(KeyData, KeySecret, RD) ->
    Config = #aws_config{access_key_id=KeyData, secret_access_key=KeySecret},
    Method = wrq:method(RD),
    %Headers = wrq:req_headers(RD),
    ContentMD5 = wrq:get_req_header("Content-MD5", RD),
    ContentType = wrq:get_req_header("Content-Type", RD),
    Date = wrq:get_req_header("Date", RD),
    %%Host = wrq:get_req_header("Host", RD),
    Resource = wrq:get_req_header("x-rcs-rewrite-path", RD),
    make_authorization(Config, Method, ContentMD5, ContentType, Date, [],
                       "", Resource, []).

%% Copy&pasted from erlcloud 0.4.5
make_authorization(Config, Method, ContentMD5, ContentType, Date, AmzHeaders,
                   Host, Resource, Subresources) ->
    CanonizedAmzHeaders =
        [[Name, $:, Value, $\n] || {Name, Value} <- lists:sort(AmzHeaders)],
    StringToSign = [string:to_upper(atom_to_list(Method)), $\n,
                    ContentMD5, $\n,
                    ContentType, $\n,
                    Date, $\n,
                    CanonizedAmzHeaders,
                    case Host of "" -> ""; _ -> [$/, Host] end,
                    Resource,
                    format_subresources(Subresources)

                   ],
    Signature = base64:encode(crypto:mac(hmac, sha, Config#aws_config.secret_access_key, StringToSign)),
    ["AWS ", Config#aws_config.access_key_id, $:, Signature].

format_subresources([]) ->
    [];
format_subresources(Subresources) ->
    [$? | string:join(lists:sort([format_subresource(Subresource) ||
                                     Subresource <- Subresources]),
                      "&")].

format_subresource({Subresource, Value}) when is_list(Value) ->
    Subresource ++ "=" ++ Value;
format_subresource({Subresource, Value}) when is_integer(Value) ->
    Subresource ++ "=" ++ integer_to_list(Value);
format_subresource(Subresource) ->
    Subresource.
