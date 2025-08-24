use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};

#[derive(Debug, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum OrderType {
    Limit,
    Market,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderState {
    Filled,
    PartiallyFilled,
    Open,
    Close, // reserved for cancelling orders, in future use
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeEvent {
    pub buyer: String,
    pub seller: String,
    pub symbol: String,
    pub quantity: u64,
    pub price: i64,
}

type PriceMap = BTreeMap<i64, VecDeque<Order>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    // pub order_id: u64,
    pub user: String,
    pub side: Side,
    pub price: Option<i64>,
    pub quantity: u64,
    // pub timestamp: i64,
    pub symbol: String,
    #[serde(default = "default_state")]
    pub state: OrderState,
}

fn default_state() -> OrderState {
    OrderState::Open
}

impl Order {
    pub fn new_limit_order(
        // order_id: u64,
        quantity: u64,
        // timestamp: i64,
        price: Option<i64>,
        side: Side,
        symbol: String,
        user: String,
    ) -> Self {
        Self {
            // order_id,
            user,
            side,
            price,
            quantity,
            // timestamp,
            state: OrderState::Open,
            symbol,
        }
    }

    pub fn new_market_order(
        // order_id: u64,
        quantity: u64,
        // timestamp: i64,
        side: Side,
        symbol: String,
        user: String,
    ) -> Self {
        Self {
            // order_id,
            user,
            side,
            price: None, // as market orders are executed based on the price from the orderbook
            quantity,
            // timestamp,
            state: OrderState::Open,
            symbol,
        }
    }
}

#[derive(Debug)]
pub struct OrderBook {
    pub bid_map: PriceMap,
    pub ask_map: PriceMap,
    pub symbol: String,
}

impl OrderBook {
    pub fn new(symbol: String) -> Self {
        Self {
            bid_map: BTreeMap::new(),
            ask_map: BTreeMap::new(),
            symbol,
        }
    }

    pub fn add_limit_order(&mut self, mut order: Order) -> Vec<TradeEvent> {
        let side = &order.side;
        let price = order.price.unwrap();
        let mut to_fill = order.quantity;

        let mut events = Vec::new();

        match side {
            Side::Buy => {
                if let Some((&lowest_ask_price, _)) = self.ask_map.first_key_value() {
                    if price >= lowest_ask_price {
                        (to_fill, events) = Self::match_orders(
                            to_fill,
                            Some(price),
                            &mut self.ask_map,
                            true,
                            OrderType::Limit,
                            order.user.as_str(),
                        );
                    }
                }
                if to_fill > 0 {
                    order.quantity = to_fill;
                    Self::insert_order(&mut self.bid_map, price, order);
                }
            }
            Side::Sell => {
                if let Some((&highest_bid_price, _)) = self.bid_map.last_key_value() {
                    if price <= highest_bid_price {
                        (to_fill, events) = Self::match_orders(
                            to_fill,
                            Some(price),
                            &mut self.bid_map,
                            false,
                            OrderType::Limit,
                            order.user.as_str(),
                        );
                    }
                }

                if to_fill > 0 {
                    order.quantity = to_fill;
                    Self::insert_order(&mut self.ask_map, price, order);
                }
            }
        };
        events
    }

    pub fn add_market_order(&mut self, order: Order) -> Vec<TradeEvent> {
        let side = &order.side;
        let remaining_quantity_to_be_filled = order.quantity;

        let (price_order_map, ascending) = match side {
            Side::Buy => (&mut self.ask_map, true),
            Side::Sell => (&mut self.bid_map, false),
        };

        let (_to_fill, events) = Self::match_orders(
            remaining_quantity_to_be_filled,
            None,
            price_order_map,
            ascending,
            OrderType::Market,
            order.user.as_str(),
        );
        events
    }

    pub fn match_orders(
        mut to_fill: u64,
        price: Option<i64>,
        book: &mut PriceMap,
        ascending: bool,
        ordertype: OrderType,
        user_id: &str,
    ) -> (u64, Vec<TradeEvent>) {
        let mut events = Vec::new();
        let keys: Vec<i64> = if ascending {
            book.keys().cloned().collect()
        } else {
            book.keys().rev().cloned().collect()
        };

        for current_price in keys {
            if let OrderType::Limit = ordertype {
                let price_cross = if ascending {
                    price.unwrap() >= current_price // Buy vs Ask
                } else {
                    price.unwrap() <= current_price // Sell vs Bid
                };

                if !price_cross {
                    break;
                }
            }

            let current_queue = book.get_mut(&current_price).unwrap();

            while to_fill > 0 {
                if let Some(mut front_order) = current_queue.pop_front() {
                    let consumed_quantity = to_fill.min(front_order.quantity);

                    // Update resting order state
                    front_order.quantity -= consumed_quantity;
                    front_order.state = if front_order.quantity == 0 {
                        OrderState::Filled
                    } else {
                        OrderState::PartiallyFilled
                    };

                    // Emit event
                    events.push(make_event(&front_order, &user_id, consumed_quantity));

                    // Put back if partially filled
                    if front_order.quantity > 0 {
                        current_queue.push_front(front_order);
                    }

                    to_fill -= consumed_quantity;
                } else {
                    break;
                }
            }

            if current_queue.is_empty() {
                book.remove(&current_price);
            }

            if to_fill == 0 {
                break;
            }
        }

        (to_fill, events)
    }

    fn insert_order(price_order_map: &mut PriceMap, price: i64, order: Order) {
        price_order_map
            .entry(price)
            .or_insert_with(VecDeque::new)
            .push_back(order);
    }
}

fn trade_parties(maker: &Order, taker_id: &str) -> (String, String) {
    match maker.side {
        Side::Buy => (maker.user.clone(), taker_id.to_string()),
        Side::Sell => (taker_id.to_string(), maker.user.clone()),
    }
}

fn make_event(maker: &Order, taker_id: &str, qty: u64) -> TradeEvent {
    let (buyer, seller) = trade_parties(maker, taker_id);
    TradeEvent {
        buyer,
        seller,
        price: maker.price.unwrap(),
        quantity: qty,
        symbol: maker.symbol.clone(),
    }
}

// ---------------------------------------------TESTS---------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn make_order(id: u64, dir: Side, qty: u64, price: i64, user_id: String) -> Order {
        Order {
            // order_id: id,
            // timestamp: id as i64,
            side: dir,
            quantity: qty,
            price: Some(price),
            state: OrderState::Open,
            symbol: String::from("AAPL"),
            user: user_id,
        }
    }

    fn make_market_order(id: u64, dir: Side, qty: u64, user_id: String) -> Order {
        Order {
            // order_id: id,
            // timestamp: id as i64,
            side: dir,
            quantity: qty,
            price: None, // irrelevant for market
            state: OrderState::Open,
            symbol: String::from("AAPL"),
            user: user_id,
        }
    }

    #[test]
    fn test_limit_orders_sit_in_book() {
        let mut book = OrderBook::new(String::from("AAPL"));

        // Insert 10 limit orders (5 buys, 5 sells)
        for i in 0..5 {
            let events = book.add_limit_order(make_order(
                i,
                Side::Buy,
                10,
                100 - i as i64,
                String::from("shyamnatesan21@gmail.com"),
            ));
            // no matches should occur, so no events
            assert!(events.is_empty());
        }
        for i in 5..10 {
            let events = book.add_limit_order(make_order(
                i,
                Side::Sell,
                10,
                101 + (i - 5) as i64,
                String::from("shyamnatesan21@gmail.com"),
            ));
            assert!(events.is_empty());
        }

        // Assertions: best bid = 100, best ask = 101
        assert_eq!(*book.bid_map.last_key_value().unwrap().0, 100);
        assert_eq!(*book.ask_map.first_key_value().unwrap().0, 101);
        assert_eq!(book.bid_map.values().map(|q| q.len()).sum::<usize>(), 5);
        assert_eq!(book.ask_map.values().map(|q| q.len()).sum::<usize>(), 5);
    }

    #[test]
    fn test_full_fill_limit_vs_limit() {
        let mut book = OrderBook::new(String::from("AAPL"));

        // Seed asks (10 sell orders at prices 100..109, qty 5 each)
        for i in 0..10 {
            let events = book.add_limit_order(make_order(
                i,
                Side::Sell,
                5,
                100 + i as i64,
                String::from("shyamnatesan21@gmail.com"),
            ));
            assert!(events.is_empty()); // no trades yet
        }

        // // Incoming buy order at 110 for qty 50(should sweep lowest asks fully)
        let events = book.add_limit_order(make_order(
            99,
            Side::Buy,
            50,
            110,
            String::from("monishnatesan17@gmail.com"),
        ));

        // It should generate trades for all 10 asks (5 qty each) = 50 qty total
        assert_eq!(events.len(), 10);

        // Verify quantities sum up correctly
        let total_qty: i64 = events.iter().map(|e| e.quantity as i64).sum();
        assert_eq!(total_qty, 50);

        // Compute weighted average trade price
        let total_notional: i64 = events.iter().map(|e| e.price * e.quantity as i64).sum();
        let average_price = total_notional as f64 / total_qty as f64;

        assert_eq!((average_price - 104.5).abs(), 0.0);

        // After execution, 0 asks remain up to 109
        assert!(book.ask_map.range(..=109).all(|(_, q)| q.is_empty()));
    }

    #[test]
    fn test_partial_fill_large_buy() {
        let mut book = OrderBook::new(String::from("AAPL"));

        // Seed 10 asks with 10 qty each
        for i in 0..10 {
            let events = book.add_limit_order(make_order(
                i,
                Side::Sell,
                10,
                100 + i as i64,
                String::from("shyamnatesan21@gmail.com"),
            ));
            assert!(events.is_empty()); // seeding should not trigger trades
        }

        // Incoming large buy of 150 at 110
        let events = book.add_limit_order(make_order(
            200,
            Side::Buy,
            150,
            110,
            String::from("monishnatesan17@gmail.com"),
        ));

        // It should consume all 100 shares from asks [100..109], but leave 50 unfilled
        let total_filled: i64 = events.iter().map(|e| e.quantity as i64).sum();
        assert_eq!(total_filled, 100);

        // That leftover 50 should sit in bid book at price 110
        let bid_q = book.bid_map.get(&110).unwrap();
        assert_eq!(bid_q.front().unwrap().quantity, 50);
    }

    #[test]
    fn test_market_orders_sweep() {
        let mut book = OrderBook::new(String::from("AAPL"));

        // Seed 10 asks of 10 qty each (prices 100..109)
        for i in 0..10 {
            let events = book.add_limit_order(make_order(
                i,
                Side::Sell,
                10,
                100 + i as i64,
                String::from("shyamnatesan21@gmail.com"),
            ));
            assert!(events.is_empty()); // limit orders don't immediately match
        }

        // Incoming market buy of 60
        let events = book.add_market_order(make_market_order(
            500,
            Side::Buy,
            60,
            String::from("monishnatesan17@gmail.com"),
        ));

        // Check total filled = 60
        let total_filled: u64 = events.iter().map(|e| e.quantity).sum();
        assert_eq!(total_filled, 60);

        // Compute average trade price
        let total_notional: i64 = events.iter().map(|e| e.price * e.quantity as i64).sum();
        let average_price = total_notional as f64 / total_filled as f64;
        assert_eq!((average_price - 102.5).abs(), 0.0);

        // Remaining asks should reflect 40 left
        let total_remaining: u64 = book
            .ask_map
            .values()
            .map(|q| q.iter().map(|o| o.quantity).sum::<u64>())
            .sum();
        assert_eq!(total_remaining, 40);
    }

    #[test]
    fn test_mixed_complex_flow() {
        let mut book = OrderBook::new(String::from("AAPL"));

        // Step 1: add 5 buys
        for i in 0..5 {
            let events = book.add_limit_order(make_order(
                i,
                Side::Buy,
                10,
                100 - i as i64,
                format!("buyer{i}@test.com"),
            ));
            assert!(events.is_empty());
        }
        // Step 2: add 5 sells
        for i in 5..10 {
            let events = book.add_limit_order(make_order(
                i,
                Side::Sell,
                10,
                101 + (i - 5) as i64,
                format!("seller{i}@test.com"),
            ));
            assert!(events.is_empty());
        }

        // Step 3: Add crossing buy at 105 (should eat ask at 101,102,...)
        let events = book.add_limit_order(make_order(
            20,
            Side::Buy,
            25,
            105,
            "crossbuyer@test.com".to_string(),
        ));
        let total_qty: u64 = events.iter().map(|e| e.quantity).sum();
        assert_eq!(total_qty, 25);
        let total_notional: i64 = events.iter().map(|e| e.price * e.quantity as i64).sum();
        let avg_price = total_notional as f64 / total_qty as f64;
        assert_eq!((avg_price - 101.8).abs(), 0.0);

        // Step 4: Market sell of 30, consuming from bid side (100..96)
        let events = book.add_market_order(make_market_order(
            21,
            Side::Sell,
            30,
            "marketseller@test.com".to_string(),
        ));
        let total_qty: u64 = events.iter().map(|e| e.quantity).sum();
        assert_eq!(total_qty, 30);
        let total_notional: i64 = events.iter().map(|e| e.price * e.quantity as i64).sum();
        let avg_price = total_notional as f64 / total_qty as f64;
        assert_eq!((avg_price - 99.0).abs(), 0.0);

        // Best ask should now be 103
        assert_eq!(*book.ask_map.first_key_value().unwrap().0, 103);

        // Step 5: Big buy sweep (1000 qty) — only 25 ask qty left
        let events = book.add_market_order(make_market_order(
            22,
            Side::Buy,
            1000,
            "bigbuyer@test.com".to_string(),
        ));

        let total_qty: u64 = events.iter().map(|e| e.quantity).sum();
        assert_eq!(total_qty, 25); // only 25 left to take

        let total_notional: i64 = events.iter().map(|e| e.price * e.quantity as i64).sum();
        let avg_price = total_notional as f64 / total_qty as f64;
        assert_eq!((avg_price - 104.2).abs(), 0.0);

        // Assertions: no asks left
        assert!(book.ask_map.values().all(|q| q.is_empty()));

        // Bid side should still have resting bids
        assert_eq!(book.bid_map.values().map(|q| q.len()).sum::<usize>(), 2);

        // best bid = 97
        assert_eq!(*book.bid_map.last_key_value().unwrap().0, 97);
    }

    #[test]
    fn test_complex_order_flow_one() {
        let mut book = OrderBook::new(String::from("AAPL"));

        // Place 15 limit orders (spread across price levels, some clustered)
        let limits = vec![
            (Side::Sell, 100, 5),
            (Side::Sell, 100, 10),
            (Side::Sell, 102, 20),
            (Side::Sell, 105, 15),
            (Side::Sell, 110, 25),
            (Side::Sell, 110, 30),
            (Side::Sell, 115, 40),
            (Side::Buy, 95, 20),
            (Side::Buy, 95, 15),
            (Side::Buy, 94, 10),
            (Side::Buy, 92, 30),
            (Side::Buy, 90, 50),
            (Side::Buy, 85, 40),
            (Side::Buy, 85, 10),
            (Side::Buy, 80, 60),
        ];

        for (i, (dir, price, qty)) in limits.into_iter().enumerate() {
            book.add_limit_order(make_order(
                i as u64,
                dir,
                qty,
                price,
                format!("user{i}@test.com"),
            ));
        }

        // Add 10 market orders interleaved
        let markets = vec![
            (Side::Buy, 15),
            (Side::Buy, 25),
            (Side::Sell, 10),
            (Side::Sell, 35),
            (Side::Buy, 50),
            (Side::Sell, 20),
            (Side::Buy, 60),
            (Side::Sell, 30),
            (Side::Buy, 40),
            (Side::Sell, 25),
        ];

        for (i, (dir, qty)) in markets.into_iter().enumerate() {
            book.add_market_order(make_market_order(
                1000 + i as u64,
                dir,
                qty,
                format!("mktuser{i}@test.com"),
            ));
        }

        // Assertions: order book should remain consistent
        assert_eq!(*book.bid_map.last_key_value().unwrap().0, 90);
        assert_eq!(book.ask_map.is_empty(), true);

        // Ensure at least some quantities remain on both sides
        let total_bids: u64 = book
            .bid_map
            .values()
            .map(|q| q.iter().map(|o| o.quantity).sum::<u64>())
            .sum();
        let total_asks: u64 = book
            .ask_map
            .values()
            .map(|q| q.iter().map(|o| o.quantity).sum::<u64>())
            .sum();

        assert_eq!(total_bids, 115);
        assert_eq!(total_asks, 0);
    }
}
