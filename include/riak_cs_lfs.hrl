%% -------------------------------------------------------------------
%%
%% Copyright (c) 2012 Basho Technologies, Inc.  All Rights Reserved.
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
%% -------------------------------------------------------------------

%% 16 bits & 1MB block size = 64GB max object size
%% 24 bits & 1MB block size = 16TB max object size
%% 32 bits & 1MB block size = 4PB max object size
-define(BLOCK_FIELD_SIZE, 32).

%% druuid:v4() uses 16 bytes in raw form.
-define(UUID_BYTES, 16).

-define(OBJECT_BUCKET_PREFIX, <<"0o:">>).       % Version # = 0
-define(BLOCK_BUCKET_PREFIX_V0, <<"0b:">>).     % Version # = 0
-define(BLOCK_BUCKET_PREFIX_V1, <<"1b:">>).     % Version # = 1
-define(BLOCK_BUCKET_PREFIX_V2, <<"2b:">>).     % Version # = 2

%% Default maximum number of fixed-size chunks per on-disk file
%% Overridden by 'fs2_backend_max_blocks_per_file' app env var.
-define(FS2_CONTIGUOUS_BLOCKS, 128).
%% (N.B.: The block size ('fs2_backend_block_size' app env var)
%% does not have a predefined size yet.)
