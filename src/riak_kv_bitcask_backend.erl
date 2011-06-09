%% -------------------------------------------------------------------
%%
%% riak_kv_bitcask_backend: Bitcask Driver for Riak
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_kv_bitcask_backend).
-behavior(riak_kv_backend).
-author('Andy Gross <andy@basho.com>').
-author('Dave Smith <dizzyd@basho.com>').

%% KV Backend API
-export([start/2,
         stop/1,
         get/2,
         put/3,
         delete/2,
         list/1,
         list_bucket/2,
         fold/3,
         fold_keys/3,
         fold_bucket_keys/4,
         drop/1,
         is_empty/1,
         callback/3]).

%% Helper API
-export([key_counts/0]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("bitcask/include/bitcask.hrl").

-define(MERGE_CHECK_INTERVAL, timer:minutes(3)).

start(Partition, Config) ->
    StartTime = erlang:now(),
    
    %% Get the data root directory
    DataDir =
        case proplists:get_value(data_root, Config) of
            undefined ->
                case application:get_env(bitcask, data_root) of
                    {ok, Dir} ->
                        Dir;
                    _ ->
                        riak:stop("bitcask data_root unset, failing")
                end;
            Value ->
                Value
        end,

    %% Setup actual bitcask dir for this partition
    BitcaskRoot = filename:join([DataDir,
                                 integer_to_list(Partition)]),
    case filelib:ensure_dir(BitcaskRoot) of
        ok ->
            ok;
        {error, Reason} ->
            error_logger:error_msg("Failed to create bitcask dir ~s: ~p\n",
                                   [BitcaskRoot, Reason]),
            riak:stop("riak_kv_bitcask_backend failed to start.")
    end,

    BitcaskOpts = [{read_write, true}|Config],
    RV = case bitcask:open(BitcaskRoot, BitcaskOpts) of
        Ref when is_reference(Ref) ->
            schedule_merge(Ref),
            maybe_schedule_sync(Ref),
            {ok, {Ref, BitcaskRoot}};
        {error, Reason2} ->
            {error, Reason2}
    end,
    riaktor_metrics:notify(?MODULE, start, histogram,
        timer:now_diff(erlang:now(), StartTime)/1000),
    riaktor_metrics:notify(?MODULE, start, counter, 1),
    riaktor_metrics:notify(?MODULE, events, history, io_lib:format("start(~p, ~p)", 
        [Partition, Config])),
    RV.


stop({Ref, _}) ->
    StartTime = erlang:now(),
    RV = bitcask:close(Ref),
    riaktor_metrics:notify(?MODULE, stop, histogram,
        timer:now_diff(erlang:now(), StartTime)/1000),
    riaktor_metrics:notify(?MODULE, stop, counter, 1),
    riaktor_metrics:notify(?MODULE, events, history, io_lib:format("stop(~p, _)", 
        [Ref])),
    RV.


get({Ref, _}, BKey) ->
    StartTime = erlang:now(),
    Key = term_to_binary(BKey),
    RV = case bitcask:get(Ref, Key) of
        {ok, Value} ->
            {ok, Value};
        not_found  ->
            {error, notfound};
        {error, Reason} ->
            {error, Reason}
    end,
    riaktor_metrics:notify(?MODULE, get, histogram,
        timer:now_diff(erlang:now(), StartTime)/1000),
    riaktor_metrics:notify(?MODULE, get, counter, 1),
    RV.

put({Ref, _}, BKey, Val) ->
    StartTime = erlang:now(),
    Key = term_to_binary(BKey),
    RV =  bitcask:put(Ref, Key, Val),
    riaktor_metrics:notify(?MODULE, put, histogram,
        timer:now_diff(erlang:now(), StartTime)/1000),
    riaktor_metrics:notify(?MODULE, put, counter, 1),
    ok = RV.

delete({Ref, _}, BKey) ->
    StartTime = erlang:now(),
    RV = bitcask:delete(Ref, term_to_binary(BKey)),
    riaktor_metrics:notify(?MODULE, del, histogram,
        timer:now_diff(erlang:now(), StartTime)/1000),
    riaktor_metrics:notify(?MODULE, del, counter, 1),
    ok = RV.

list({Ref, _}) ->
    StartTime = erlang:now(),
    RV = case bitcask:list_keys(Ref) of
        KeyList when is_list(KeyList) ->
            [binary_to_term(K) || K <- KeyList];
        Other ->
            Other
    end,
    riaktor_metrics:notify(?MODULE, list, histogram,
        timer:now_diff(erlang:now(), StartTime)/1000),
    riaktor_metrics:notify(?MODULE, list, counter, 1),
    riaktor_metrics:notify(?MODULE, events, history, io_lib:format("list(~p, _)", 
        [Ref])),
    RV.

list_bucket({Ref, _}, {filter, Bucket, Fun}) ->
    bitcask:fold_keys(Ref,
        fun(#bitcask_entry{key=BK},Acc) ->
                {B,K} = binary_to_term(BK),
		case (B =:= Bucket) andalso Fun(K) of
		    true ->
			[K|Acc];
		    false ->
                        Acc
                end
        end, []);
list_bucket({Ref, _}, '_') ->
    bitcask:fold_keys(Ref,
        fun(#bitcask_entry{key=BK},Acc) ->
                {B,_K} = binary_to_term(BK),
                case lists:member(B,Acc) of
                    true -> Acc;
                    false -> [B|Acc]
                end
        end, []);
list_bucket({Ref, _}, Bucket) ->
    bitcask:fold_keys(Ref,
        fun(#bitcask_entry{key=BK},Acc) ->
                {B,K} = binary_to_term(BK),
                case B of
                    Bucket -> [K|Acc];
                    _ -> Acc
                end
        end, []).

fold({Ref, _}, Fun0, Acc0) ->
    %% When folding across the bitcask, the bucket/key tuple must
    %% be decoded. The intermediate binary_to_term call handles this
    %% and yields the expected fun({B, K}, Value, Acc)
    bitcask:fold(Ref,
                 fun(K, V, Acc) ->
                         Fun0(binary_to_term(K), V, Acc)
                 end,
                 Acc0).

fold_keys({Ref, _}, Fun, Acc) ->
    StartTime = erlang:now(),
    F = fun(#bitcask_entry{key=K}, Acc1) ->
                Fun(binary_to_term(K), Acc1) end,
    RV = bitcask:fold_keys(Ref, F, Acc),
    riaktor_metrics:notify(?MODULE, fold_keys, histogram,
        timer:now_diff(erlang:now(), StartTime)/1000),
    riaktor_metrics:notify(?MODULE, fold_keys, counter, 1),
    riaktor_metrics:notify(?MODULE, events, history, io_lib:format("fold_keys({~p, _}, ~p, length(Acc) = ~p)", 
        [Ref, Fun, length(Acc)])),
    RV.

fold_bucket_keys(ModState, _Bucket, Fun, Acc) ->
    fold_keys(ModState, fun(Key2, Acc2) -> Fun(Key2, dummy_val, Acc2) end, Acc).

drop({Ref, BitcaskRoot}) ->
    %% todo: once bitcask has a more friendly drop function
    %%  of its own, use that instead.
    bitcask:close(Ref),
    {ok, FNs} = file:list_dir(BitcaskRoot),
    [file:delete(filename:join(BitcaskRoot, FN)) || FN <- FNs],
    file:del_dir(BitcaskRoot),
    riaktor_metrics:notify(?MODULE, drop, counter, 1),
    riaktor_metrics:notify(?MODULE, events, history, io_lib:format("drop({~p, ~p})", 
        [Ref, BitcaskRoot])),
    ok.

is_empty({Ref, _}) ->
    %% Determining if a bitcask is empty requires us to find at least
    %% one value that is NOT a tombstone. Accomplish this by doing a fold_keys
    %% that forcibly bails on the very first key encountered.
    StartTime = erlang:now(),
    F = fun(_K, _Acc0) ->
                throw(found_one_value)
        end,
    RV = (catch bitcask:fold_keys(Ref, F, undefined)) /= found_one_value,
    riaktor_metrics:notify(?MODULE, is_empty, histogram,
        timer:now_diff(erlang:now(), StartTime)/1000),
    riaktor_metrics:notify(?MODULE, is_empty, counter, 1),
    riaktor_metrics:notify(?MODULE, events, history, io_lib:format("is_empty({~p, _})", 
        [Ref])),
    RV.

callback({Ref, _}, Ref, {sync, SyncInterval}) when is_reference(Ref) ->
    StartTime = erlang:now(),
    bitcask:sync(Ref),
    RV = schedule_sync(Ref, SyncInterval),
    riaktor_metrics:notify(?MODULE, sync, histogram,
        timer:now_diff(erlang:now(), StartTime)/1000),
    riaktor_metrics:notify(?MODULE, sync, counter, 1),
    riaktor_metrics:notify(?MODULE, events, history, io_lib:format("callback[sync]({~p, _}), {sync, SyncInterval = ~p}", 
        [Ref, SyncInterval])),
    RV;
callback({Ref, BitcaskRoot}, Ref, merge_check) when is_reference(Ref) ->
    StartTime = erlang:now(),
    case bitcask:needs_merge(Ref) of
        {true, Files} ->
            bitcask_merge_worker:merge(BitcaskRoot, [], Files);
        false ->
            ok
    end,
    RV = schedule_merge(Ref),
    riaktor_metrics:notify(?MODULE, merge_check, histogram,
        timer:now_diff(erlang:now(), StartTime)/1000),
    riaktor_metrics:notify(?MODULE, merge_check, counter, 1),
    riaktor_metrics:notify(?MODULE, events, history, io_lib:format("callback[merge_check]({~p, ~p})", 
        [Ref, BitcaskRoot])),
    RV;
%% Ignore callbacks for other backends so multi backend works
callback(_State, _Ref, _Msg) ->
    ok.

key_counts() ->
    case application:get_env(bitcask, data_root) of
        {ok, RootDir} ->
            [begin
                 {Keys, _} = status(filename:join(RootDir, Dir)),
                 {Dir, Keys}
             end || Dir <- element(2, file:list_dir(RootDir))];
        undefined ->
            {error, data_root_not_set}
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
%% Invoke bitcask:status/1 for a given directory
status(Dir) ->
    Ref = bitcask:open(Dir),
    try bitcask:status(Ref)
    after bitcask:close(Ref)
    end.      

%% @private
%% Schedule sync (if necessary)
maybe_schedule_sync(Ref) when is_reference(Ref) ->
    case application:get_env(bitcask, sync_strategy) of
        {ok, {seconds, Seconds}} ->
            SyncIntervalMs = timer:seconds(Seconds),
            schedule_sync(Ref, SyncIntervalMs);
            %% erlang:send_after(SyncIntervalMs, self(),
            %%                   {?MODULE, {sync, SyncIntervalMs}});
        {ok, none} ->
            ok;
        BadStrategy ->
            error_logger:info_msg("Ignoring invalid bitcask sync strategy: ~p\n",
                                  [BadStrategy]),
            ok
    end.

schedule_sync(Ref, SyncIntervalMs) when is_reference(Ref) ->
    riak_kv_backend:callback_after(SyncIntervalMs, Ref, {sync, SyncIntervalMs}).

schedule_merge(Ref) when is_reference(Ref) ->
    riak_kv_backend:callback_after(?MERGE_CHECK_INTERVAL, Ref, merge_check).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

simple_test() ->
    ?assertCmd("rm -rf test/bitcask-backend"),
    application:set_env(bitcask, data_root, "test/bitcask-backend"),
    riak_kv_backend:standard_test(?MODULE, []).

custom_config_test() ->
    ?assertCmd("rm -rf test/bitcask-backend"),
    application:set_env(bitcask, data_root, ""),
    riak_kv_backend:standard_test(?MODULE, [{data_root, "test/bitcask-backend"}]).

-endif.
