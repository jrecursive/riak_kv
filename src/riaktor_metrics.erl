%% -------------------------------------------------------------------
%%
%% riaktor_metrics: Functions supporting Riaktor metrics collection
%%
%% Copyright (c) 2007-2010 John Muellerleile  All Rights Reserved.
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
-module(riaktor_metrics).
-export([
        init/0,
        notify/4
        ]).

metrics() -> [
    {riak_kv_vnode, [
        {init, counter},
        {get, counter},
        {mget, counter},
        {del, counter},
        {put, counter},
        {readrepair, counter},
        {list_keys, counter},
        {fold, counter},
        {get_vclocks, counter},
        {backend_callback, counter},
        {mapexec_error_noretry, counter},
        {mapexec_reply, counter},
        {handoff_started, counter},
        {handoff_cancelled, counter},
        {handoff_finished, counter},
        {list_bucket, counter},

        {init, histogram},
        {get, histogram},
        {mget, histogram},
        {del, histogram},
        {put, histogram},
        {readrepair, histogram},
        {list_keys, histogram},
        {fold, histogram},
        {backend_callback, histogram},
        {mapexec_error_noretry, histogram},
        {mapexec_reply, histogram},
        {handoff_started, histogram},
        {handoff_cancelled, histogram},
        {handoff_finished, histogram},
        {list_bucket, histogram},
        
        {events, history, [5000]}
    ]},
    {riak_kv_bitcask_backend, [
        {start, counter},
        {stop, counter},
        {get, counter},
        {put, counter},
        {del, counter},
        {list, counter},
        {fold_keys, counter},
        {drop, counter},
        {is_empty, counter},
        {sync, counter},
        {merge_check, counter},

        {start, histogram},
        {stop, histogram},
        {get, histogram},
        {put, histogram},
        {del, histogram},
        {list, histogram},
        {fold_keys, histogram},
        {drop, histogram},
        {is_empty, histogram},
        {sync, histogram},
        {merge_check, histogram},

        {events, history, [5000]}        
    ]}].

%% Public        

init() ->
    lists:foreach(fun({OriginModule, Metrics}) ->
        lists:foreach(fun(V) ->
            io:format("~p: init: ~p~n", [?MODULE, begin case V of
                {Name, Type, Args} -> new_metric(OriginModule, Name, Type, Args);
                {Name, Type} -> new_metric(OriginModule, Name, Type, [])
        end end]) end, Metrics) end, metrics()).

notify(Mod, Name, counter, {Op, Val}) when is_atom(Op) ->
    folsom_metrics:notify({format_name(Mod, Name, counter), {Op, Val}});
notify(Mod, Name, counter, Val) when is_number(Val) ->
    notify(Mod, Name, counter, {inc, Val});
notify(Mod, Name, histogram, Val) when is_number(Val) ->
    folsom_metrics:notify({format_name(Mod, Name, histogram), Val});
notify(Mod, Name, gauge, Val) when is_number(Val) ->
    folsom_metrics:notify({format_name(Mod, Name, gauge), Val});
notify(Mod, Name, meter, Val) when is_number(Val) ->
    folsom_metrics:notify({format_name(Mod, Name, meter), Val});
notify(Mod, Name, history, Event) ->
    folsom_metrics:notify({format_name(Mod, Name, history), Event});
notify(Mod, Name, Type, Val) ->
    io:format("~p:notify(~p, ~p, ~p, ~p): Unknown type!~n",
        [?MODULE, Mod, Name, Type, Val]).

%% Internal

new_metric(OriginModule, Name, Type, Args) ->
    Fun = list_to_atom(lists:flatten(io_lib:format("new_~p", [Type]))),
    Name0 = format_name(OriginModule, Name, Type),
    io:format("~p:new_metric(~p, ~p, ~p, ~p)~n", [?MODULE, OriginModule, Name, Type, Args]),
    erlang:apply(folsom_metrics, Fun, [Name0] ++ Args   ).

format_name(OriginModule, Name, Type) ->
    list_to_atom(lists:flatten(io_lib:format("~p,~p,~p", [OriginModule, Name, Type]))).

