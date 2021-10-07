-module(antidote_cache_driver).
-behaviour(rcl_bench_driver).

-export([new/1, run/4, terminate/2]).

-export([mode/0, concurrent_workers/0, duration/0, operations/0, test_dir/0,
         key_generator/0, value_generator/0, random_algorithm/0,
         random_seed/0, shutdown_on_error/0]).

%% =======================
%% Benchmark configuration

mode() -> {ok, {rate, max}}.
%% Number of concurrent workers
concurrent_workers() -> {ok, 100}.
%% Test duration (minutes)
duration() -> {ok, 1}.
%% Operations (and associated mix)
operations() ->
    {ok, [{update, 10},
	  {read, 80}
          ]}.

%% Base test output directory
test_dir() -> {ok, "tests"}.

%% Key generators
%% {uniform_int, N} - Choose a uniformly distributed integer between 0 and N
key_generator() -> {ok, {pareto_int, 10}}.

%% Value generators
%% {fixed_bin, N} - Fixed size binary blob of N bytes
value_generator() -> {ok, {fixed_bin, 100}}.

random_algorithm() -> {ok, exsss}.
random_seed() -> {ok, {1,4,3}}.

shutdown_on_error() -> false.



%% ========================
%% Benchmark implementation

new(Id) ->
	Node = list_to_atom("antidote@antidote-dc1.local"),
	State = #{id => Id, node => Node, module=> antidote},
    {ok, State}.

run(update, KeyGen, _ValueGen, #{node:=Node, module:=Mod} = State) ->
 	Key = list_to_atom(integer_to_list(KeyGen())),
	Type = antidote_crdt_counter_pn,
	Object = {Key, Type},
	Update = {Object, increment, 1},
	RESULT = rpc:call(Node, Mod, update_objects, [ignore, [], [Update]]),
	%io:format("Update Result ~p ~n",[RESULT]),
     {ok, State};

run(read, KeyGen, _ValueGen,  #{node:=Node, module:=Mod} = State) ->
	Key = list_to_atom(integer_to_list(KeyGen())),
   	Type = antidote_crdt_counter_pn,
        Object = {Key, Type},
	RESULT = rpc:call(Node, Mod, read_objects, [ignore, [], [Object]]),
	%io:format("Read Result ~p ~n",[RESULT]),
	{ok, State}.

terminate(_, State) ->
  logger:notice("Finished: ~p", [State]),
  ok.









