-module(antidote_pb_driver).
-behaviour(rcl_bench_driver).

-export([new/1, run/4, terminate/2]).

-export([mode/0, concurrent_workers/0, duration/0, operations/0, test_dir/0,
  key_generator/0, value_generator/0, random_algorithm/0,
  random_seed/0, shutdown_on_error/0, crash_is_recoverable/0]).
% generic config
mode() -> {ok, {rate, max}}.
concurrent_workers() -> {ok, 40}.
duration() -> {ok, 1}.
operations() -> {ok, [{update_only_txn, 2}, {read_only_txn, 8}]}.
%operations() -> {ok, [{txn, 1}]}.
test_dir() -> {ok, "tests"}.
key_generator() -> {ok, {uniform_int, 100000}}.
value_generator() -> {ok, {fixed_bin, 100}}.
random_algorithm() -> {ok, exsss}.
random_seed() -> {ok, {1,4,3}}.
shutdown_on_error() -> false.
crash_is_recoverable() -> true.

% antidote config
%%{operations, [{update_only_txn, 1}, {read_only_txn, 1}, {append, 1}, {read, 1}, {txn, 1} ]}.
antidote_pb_port() -> 8087.
antidote_pb_ip() -> '127.0.0.1'.
antidote_types() -> dict:from_list([{antidote_crdt_counter_pn, [{increment,1}, {decrement,1}]}]).
%%{antidote_types, [{antidote_crdt_set_aw, [add, remove]}]}.
%%{antidote_types, [{antidote_crdt_set_go, [add, remove]}]}.
%%{antidote_types, [{antidote_crdt_register_mv, [assign]}]}.
%%{antidote_types, [{antidote_crdt_register_lww, [assign]}]}.
%% Use the following parameter to set the size of the orset
set_size() -> 10.


%%%%%%%%%%%%%%%%%%%%%%%%%%%% for transactions %%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% The following parameters are used when issuing transactions.
%% When running append and read operations, they are ignored.

%% Number of reads; update_only_txn ignores it.
num_reads() -> 1.
%% Number of updates; read_only_txn ignores it.
num_updates() -> 1.

%% If sequential_reads is set to true,
%% the client will send each read (of a total
%% num_reads) in a different antidote:read_objects call.
%% when set to false, all (num_reads) reads will be sent
%% in a single read_objects call, which is faster, as
%% antidote will process them in parallel.
sequential_reads() -> false.

%% Idem for updates
-spec sequential_writes() -> false | true.
sequential_writes() -> false.

%%%%%%%%%%%%%%%%%%%%%%%%%%%% end for transactions %%%%%%%%%%%%%%%%%%%%%%%%%%%%

-define(BUCKET, <<"antidote_bench_bucket">>).

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
  KeyInfo = {integer_to_list(KeyGen()), antidote_crdt_counter_pn, ?BUCKET},
  Obj = antidotec_counter:increment(1, antidotec_counter:new()),
  _ = antidotec_pb:update_objects(Pid, antidotec_counter:to_ops(KeyInfo, Obj), TxId),
  {ok, State};

run(read_only_txn, KeyGen, _ValueGen, State=#state{pb_pid=Pid}) ->
  {ok, TxId} = antidotec_pb:start_transaction(Pid, ignore, [{static, true}]),
  Key = KeyGen(),
  Obj = {integer_to_list(Key), antidote_crdt_counter_pn, ?BUCKET},
  {ok, [R]} = antidotec_pb:read_objects(Pid, [Obj], TxId),
  {ok, State}.


terminate(_, State) ->
  logger:notice("Finished: ~p", [State]),
  ok.
