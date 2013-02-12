%% JSON keys used by rts module
-define(START_TIME, <<"StartTime">>).
-define(END_TIME, <<"EndTime">>).

%% http://docs.basho.com/riakcs/latest/cookbooks/Querying-Access-Statistics/
-type usage_field_type() :: 'Count' | 'UserErrorCount' | 'SystemErrorCount'
                          | 'BytesIn' | 'UserErrorBytesIn' | 'SystemErrorBytesIn'
                          | 'BytesOut' | 'UserErrorBytesOut' | 'SystemErrorBytesOut'
                          | 'BytesOutIncomplete'.

-define(SUPPORTED_USAGE_FIELD, ['Count' , 'UserErrorCount' , 'SystemErrorCount',
                                'BytesIn' , 'UserErrorBytesIn' , 'SystemErrorBytesIn',
                                'BytesOut' , 'UserErrorBytesOut' , 'SystemErrorBytesOut',
                                'BytesOutIncomplete']).
