-record(multipart_manifest_v1, {
    bkey :: {binary(), binary()},
    upload_id :: binary(),
    start_time :: term(),

    cluster_id :: undefined | binary(),

    acl :: term(),
    metadata :: orddict:orddict(),

    %% since we don't have any point of strong
    %% consistency (other than stanchion), we
    %% can get concurrent `complete' and `abort'
    %% requests. There are still some details to
    %% work out, but what we observe here will
    %% affect whether we accept future `complete'
    %% or `abort' requests.

    %% set of complete_request()
    %% TODO: should this be an orddict
    %% instead of a set so we can
    %% retrieve the complete
    %% record by some index?
    complete_requests :: ordsets:ordset(),

    %% set of abort_request()
    %% TODO: same note as above
    abort_requests :: ordsets:ordset(),

    %% Stores references to all of the parts uploaded
    %% with this `upload_id' so far. Since a part
    %% can be uploaded more than once with the same
    %% part number, we store the values in this dict
    %% as sets instead of just `part_manifest()'.
    %% [{integer(), ordsets:ordset(part_manifest())}]
    parts :: orddict:orddict(),

    %% a place to stuff future information
    %% without having to change
    %% the record format
    props = [] :: proplists:proplist()
}).
-type multipart_manifest() :: #multipart_manifest_v1{}.

-record(complete_request_v1, {

    complete_time :: term(),

    %% just an ID to refer to a specific
    %% complete record from within a collection
    id :: binary(),

    %% This is the information provided from the
    %% user from a 'complete upload' request.
    %% Once this record is made, the info has
    %% already been validated.
    %% [{PartNumber :: integer(),
    %%   Etag :: binary()}]
    part_list :: ordsets:ordset(),

    %% This is a calculatable field,
    %% not sure if it should
    %% be denormalized or not.
    %% I suppose it might
    %% as well be.
    content_length :: non_neg_integer()
}).
-type complete_request() :: #complete_request_v1{}.

-record(abort_request_v1, {
    abort_time :: term(),

    %% just an ID to refer to a specific
    %% abort record from within a collection
    id :: binary()

    %% we probably want some information
    %% here about the parts
    %% that were observed when this
    %% abort call came through.
}).
-type abort_request() :: #abort_request_v1{}.

-record(part_manifest_v1, {
    %% TODO: should this be bkey {binary(), binary()}?
    bucket :: binary(),
    key :: binary(),

    %% used to judge races between concurrent uploads
    %% of the same part_number
    start_time :: term(),

    %% still some questions here
    %% need to account for aborts?
    state :: writing | active | pending_delete | marked_delete,

    %% the parent upload identifier
    upload_id :: binary(),

    %% one-of 1-10000, inclusive
    part_number :: integer(),

    %% a UUID to prevent conflicts with concurrent
    %% uploads of the same {upload_id, part_number}.
    part_id :: binary(),

    last_block_written_time :: integer(),
    write_blocks_remaining :: orddsets:ordset(),

    %% this serves as the etag for the part
    md5 :: binary(),

    %% each individual part upload always has a content-length
    content_length :: integer(),

    %% block size just like in `lfs_manifest_v2'. Concievably,
    %% parts for the same upload id could have different block_sizes.
    block_size :: integer()
}).
-type part_manifest() :: #part_manifest_v1{}.
