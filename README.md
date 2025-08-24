A naive implementation of a centralized exchange.

centralized-exchange -> A simple http server
matching_engine -> a redis consumer
orderbook -> an orderbook datastructure implementation

Design:
<img width="1648" height="1224" alt="image" src="https://github.com/user-attachments/assets/ecb89f33-0d39-4e90-8e08-51068a0b5476" />

Usage: 
cd centralized-exchange
cargo run

in a separete terminal, go to matching_engine,
cd matching_engine
cargo run
