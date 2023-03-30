-module(brod_transaction_processor).

-include("include/brod.hrl").

%public API
-export([ do/3
        , send/4]).

% group subscriber callbacks
-export([ init/2
        , handle_message/4
        , get_committed_offsets/3]).

-export_type([ context/0
             , process_function/0]).

-opaque context() :: #{}.
-type process_function() :: fun((context(), brod:kafka_message_set()) ->   ok
                                                                         | {error, any()}).

-spec do(process_function(), brod:client(), #{}) ->   {ok, pid()}
                                                    | {error, any()}.
do(ProcessFun, Client, Opts) ->

  Defaults = #{ group_config => [{offset_commit_policy, consumer_managed}]
              , consumer_config => []},

  #{ group_id := GroupId
   , topics := Topics
   , group_config := GroupConfig
   , consumer_config := ConsumerConfig} = maps:merge(Defaults, Opts),

  InitState = #{client => Client,
                process_function => ProcessFun},

  brod:start_link_group_subscriber(client,
                                   GroupId,
                                   Topics,
                                   GroupConfig,
                                   ConsumerConfig,
                                   ?MODULE,
                                   InitState).

-spec send(context(),
           brod:topic(),
           brod:partition(),
           kpro:kafka_message_set()) ->   {ok, brod:offset()}
                                        | {error, any()}.
send(Context, Topic, Partition, Batch) ->
  brod:txn_produce(transaction(Context),
                   Topic,
                   Partition,
                   Batch).

init(GroupId, #{ client := Client
               , process_function := ProcessFun} = Opts) ->
  #{ tx_id := TxId
   , transaction_config := Config} =
  maps:merge(#{ tx_id => make_transactional_id()
              , transaction_config => []}, Opts),
  Tx = brod:transaction(Client, TxId, Config),
  {ok, #{ client => Client
        , tx => Tx
        , process_function => ProcessFun
        , group_id => GroupId}}.

handle_message(Topic,
               Partition,
               #kafka_message_set{ topic     = Topic
                                 , partition = Partition
                                 , messages  = _Messages} = MessageSet,
               #{ process_function := ProcessFun
                , tx := Tx
                , group_id := GroupId} = State) ->

  ok = ProcessFun(context(State), MessageSet),
  ok = brod:txn_add_offsets(Tx, GroupId, offsets_to_commit(MessageSet)),
  ok = brod:commit(Tx),

  {ok, ack_no_commit, State}.

get_committed_offsets(GroupId, TPs, #{client := Client} = State) ->
  {ok, Offsets} = brod:fetch_committed_offsets(Client, GroupId),
  TPOs =
  lists:filter(fun({TP, _Offset}) ->
                   lists:member(TP, TPs)
               end,
               lists:foldl(fun(#{ name := Topic
                                , partitions := Partitions}, TPOs) ->
                               lists:append(TPOs,
                                            lists:map(fun(#{ committed_offset := COffset
                                                           , partition_index := Partition}) ->
                                                          {{Topic, Partition}, COffset}
                                                      end, Partitions))
                           end, [], Offsets)),
  {ok, TPOs, State}.

%@private
make_transactional_id() ->
  iolist_to_binary([atom_to_list(?MODULE), "-txn-",
                    base64:encode(crypto:strong_rand_bytes(8))]).

context(#{} = State) -> State.

transaction(#{tx := Tx}) -> Tx.

offsets_to_commit(#kafka_message_set{ topic     = Topic
                                    , partition = Partition
                                    , messages  = Messages}) ->
  #kafka_message{offset = Offset} = lists:last(Messages),
  #{{Topic, Partition} => Offset}.


