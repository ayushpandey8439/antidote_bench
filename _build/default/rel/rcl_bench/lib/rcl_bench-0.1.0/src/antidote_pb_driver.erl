-module(antidote_pb_driver).
-behaviour(rcl_bench_driver).

-export([new/1, run/4, terminate/2]).

-export([mode/0, concurrent_workers/0, duration/0, operations/0, test_dir/0,
  key_generator/0, value_generator/0, random_algorithm/0,
  random_seed/0, shutdown_on_error/0, crash_is_recoverable/0]).
% generic config
mode() -> {ok, {rate, max}}.
concurrent_workers() -> {ok, 16}.
duration() -> {ok, 1}.
operations() -> {ok, [{inc_txn, 2},{read_only_txn, 8}]}.
%operations() -> {ok, [{txn, 1}]}.
test_dir() -> {ok, "tests"}.
key_generator() -> {ok, {pareto_int, 1000}}.
value_generator() -> {ok, {fixed_bin, 100}}.
random_algorithm() -> {ok, exsss}.
random_seed() -> {ok, {1,4,3}}.
shutdown_on_error() -> false.
crash_is_recoverable() -> true.

antidote_pb_port() -> 8087.
antidote_pb_ip() -> '192.168.1.2'.


%%%%%%%%%%%%%%%%%%%%%%%%%%%% end for transactions %%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(BUCKET, <<"antidote_bench_bucket">>).

-record(counter, {
  value :: integer(),
  increment :: integer()
}).
-opaque antidotec_counter() :: #counter{}.


-record(state, {worker_id,
  time,
  pb_pid,
  commit_time,
  num_reads,
  num_updates
}).
new(Id) ->
  io:format(user, "~nInitializing antidote bench worker~n=====================~n", []),
  io:format(user, "Using target node ~p for worker ~p~n", [antidote_pb_ip(), Id]),

  {ok, Pid} = antidotec_pb_socket:start_link(antidote_pb_ip(), antidote_pb_port()),
  io:format(user, "Connection established~n", []),

  {ok, #state{
    worker_id = Id,
    pb_pid = Pid
  }}.


run(inc_txn, KeyGen, _ValueGen, State=#state{pb_pid=Pid})->
  {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{static, true}]),
  BoundObject = {integer_to_list(KeyGen()), antidote_crdt_counter_pn, ?BUCKET},
  antidotec_pb:update_objects(Pid, [{BoundObject, increment, 1}], TxId),
{ok, State};

run(read_only_txn, KeyGen, _ValueGen, State=#state{pb_pid=Pid}) ->
  {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{static, true}]),
  Key = KeyGen(),
  Obj = {integer_to_list(Key), antidote_crdt_counter_pn, ?BUCKET},
  {ok, [R]} = antidotec_pb:read_objects(Pid, [Obj], TxId),
%	 logger:error("Result ~p",[R]),
{ok, State}.


terminate(_, State) ->
  logger:notice("Finished: ~p", [State]),
  ok.
