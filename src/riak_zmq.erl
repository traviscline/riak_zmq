-module(riak_zmq).
-export([postcommit/1]).


postcommit(RObj) ->
    riak_zmq_app:ensure_started(riak_zmq),
    {Bucket, Key, Value} = {riak_object:bucket(RObj), riak_object:key(RObj), erlang:iolist_to_binary(mochijson2:encode(riak_object:get_values(RObj)))},
    riak_zmq_publisher:publish([Bucket, Key, Value]).
