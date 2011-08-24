-module(riak_zmq_publisher).
-behaviour(gen_server).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0, publish/1]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

publish(Msg) ->
    gen_server:call(?MODULE, {publish, Msg}).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    {ok, Context} = erlzmq:context(),
    {ok, Socket} = erlzmq:socket(Context, pub),
    erlzmq:bind(Socket, app_helper:get_env(riak_zmq, zmq_uri, "tcp://127.0.0.1:5500")),
    {ok, Socket}.

handle_call({publish, Msg}, _From, Socket) ->
    send_multipart(Socket, Msg),
    {reply, ok, Socket};

handle_call(_Request, _From, State) ->
    {noreply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

send_multipart(Socket, [Next|Rest]) when Rest == [] -> 
    erlzmq:send(Socket, Next);

send_multipart(Socket, [Next|Rest]) ->
    erlzmq:send(Socket, Next, [sndmore]),
    send_multipart(Socket, Rest).
