# DKVS
___Distributed fault-tolerant key-value storage___

This work is an application of [Multi-Paxos protocol](http://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf).

How to run
==========
1. Build Gradle task `installApp`;
2. Run `build/install/dkvs_node/bin/dkvs_node <N>`, where `<N>` is node id.

To change DKVS configuration, edit `src/main/resources/dkvs.properties` 
and rebuild the project.

Client interface
====
Client may connect to any of the configured nodes, then the messages are:

`get <key>` ⇒ `VALUE <key> <value>` or `NOT_FOUND`

`set <key> <value>` ⇒ `STORED`

`delete <key>` ⇒ `DELETED` or `NOT_FOUND`

Assuming that `<key>` doesn't contain whitespaces, 
`<value>` doesn't contain line breaks.