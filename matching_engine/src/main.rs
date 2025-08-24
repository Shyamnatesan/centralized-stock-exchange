use orderbook::{Order, OrderBook};
use redis::{Client, Commands};
use std::collections::HashMap;

const ORDER_INBOUND_CHANNEL: &str = "order_inbound";
const ORDER_OUTBOUND_CHANNEL: &str = "order_outbound";

pub struct MatchingEngine {
    engine_map: HashMap<String, OrderBook>,
    redis_client: Client,
}

impl MatchingEngine {
    pub fn new(symbols: Vec<String>) -> Self {
        let mut engine_map = HashMap::new();
        for symbol in symbols.into_iter() {
            engine_map.insert(symbol.clone(), OrderBook::new(symbol));
        }
        let redis_client = redis::Client::open("redis://127.0.0.1/").unwrap();
        Self {
            engine_map,
            redis_client,
        }
    }

    pub fn run(&mut self) {
        let mut conn = self.redis_client.get_connection().unwrap();
        let mut pub_sub = conn.as_pubsub();

        pub_sub.subscribe(ORDER_INBOUND_CHANNEL).unwrap();
        println!("Running matching engine...");
        loop {
            let msg = pub_sub.get_message().unwrap();
            let payload: String = msg.get_payload().unwrap();

            match serde_json::from_str::<Order>(&payload) {
                Ok(order) => {
                    println!("Received order: {:?}", order);
                    let engine = self.engine_map.get_mut(&order.symbol).unwrap();
                    let events = match order.price {
                        Some(_) => engine.add_limit_order(order),
                        None => engine.add_market_order(order),
                    };

                    for event in events {
                        let serialzied = serde_json::to_string(&event).unwrap();
                        self.redis_client
                            .publish(ORDER_OUTBOUND_CHANNEL, serialzied)
                            .unwrap()
                    }
                }
                Err(e) => {
                    eprintln!("Failed to parse order: {} | Raw: {}", e, payload);
                }
            }
        }
    }
}

fn main() {
    let symbols = vec![
        String::from("AAPL"),
        String::from("MSFT"),
        String::from("TSLA"),
        String::from("GOOGL"),
        String::from("META"),
        String::from("INTC"),
        String::from("JPM"),
        String::from("AMZN"),
    ];
    let mut engine = MatchingEngine::new(symbols);
    engine.run()
}
