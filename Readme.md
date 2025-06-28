# Loki - Should have binaries that pass the maelstrom test suite.

### Maelstrom
The test suite can be found and installed though this link [here](https://github.com/jepsen-io/maelstrom?tab=readme-ov-file).


### Flyio Gosssip Glomers
I learnt about it through some challenge that can be found
[here](https://fly.io/dist-sys/).


#### Challenges

- This test sofware works on stdin and stdout and consumes/spits out json messages and we need to do something with it. The protocol can be found [here](https://github.com/jepsen-io/maelstrom/blob/main/resources/protocol-intro.md).

1. Init -> So the nodes are initialized with init messages and we need to handel that. 


##### Echo challenge.
- If you're running this codebase is in src/bin/echo.rs
- run lein run test - w --bin loki/target/debug/echo --node-count 3 -- time-limit 10


##### Unique_id  challenge 
- If you're running this codebase it is in src/bin/id_generator.rs
- run lein run test - w --bin loki/target/debug/id_generator --node-count 3 -- time-limit 30 --rate 1000 --availability total --nemesis partition

##### Broadcast
