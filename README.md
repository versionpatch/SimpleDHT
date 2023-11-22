# SimpleDHT

## This is a personal project where I try to build a distributed system myself from scratch. It is not meant to be used in practice and is not secure.

Peer to Peer, all the machines know each other.

Uses consistent hashing for data distribution.

Only supports single key transactions. Uses two phase commit and ensures strong consistency in normal functioning state. 

The data redistributes itself appropriately if a machine fails or joins.

If an instance fails, some instances may be out of date for a while until they are synchronized.





