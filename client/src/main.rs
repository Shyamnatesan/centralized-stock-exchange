use std::{fs::File, io::BufReader};

use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::{Duration, sleep};

#[derive(Serialize)]
struct CreateUserRequest {
    email: String,
}

#[derive(Serialize, Deserialize)]
struct PlaceOrderRequest {
    user: String,
    symbol: String,
    side: String, // "buy" or "sell"
    price: Option<i64>,
    quantity: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = Client::new();
    let base_url = "http://localhost:8080"; // adjust to your server

    // 1. Create 10 users
    let mut user_ids = vec![];
    for i in 0..10 {
        let email = format!("user{}@gmail.com", i);
        let req = CreateUserRequest { email };
        let res = client
            .post(format!("{}/user", base_url))
            .json(&req)
            .send()
            .await?
            .text()
            .await?;
        println!("Created user response: {}", res);

        // assuming server returns the user_id as plain number or JSON
        let user_id: String = serde_json::from_str(&res)?;
        user_ids.push(user_id);
    }

    // 2. Load trades from JSON file
    let file = File::open("trades.json")?;
    let reader = BufReader::new(file);
    let mut trades_json: Vec<Value> = serde_json::from_reader(reader)?;
    for trade in &mut trades_json {
        if let Some(price) = trade.get_mut("price") {
            if let Some(p) = price.as_f64() {
                *price = Value::from((p * 100.0).round() as u64);
            }
        }
    }

    let trades: Vec<PlaceOrderRequest> = serde_json::from_value(Value::Array(trades_json))?;
    println!("Loaded {} trades", trades.len());

    // 3. Loop through trades and call place_order API
    for trade in trades {
        // Map trade.user to a user_id if needed
        // For example, if trade.user = "user1@gmail.com", convert to 1
        let res = client
            .post(format!("{}/place_order", base_url))
            .json(&trade)
            .send()
            .await?
            .text()
            .await?;
        println!("Placed order response: {}", res);

        // Optional: add small delay to simulate real-world traffic
        sleep(Duration::from_millis(10)).await;
    }

    Ok(())
}
