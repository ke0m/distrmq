# distrmq
Lightweight distributed computing with ZMQ sockets. 

Provides user with the capability of distributing data over many nodes (first compressed and serialized and then transferred over TCP sockets), remotely executing a function on these data (remote procedural call) and then gathering the results back to a main node (uses a REP/REQ model from ZMQ).

Thanks to Marc Dupont as I mostly followed [his blog](http://mdup.fr/blog/easy-cluster-parallelization-with-zeromq) in implementing this
