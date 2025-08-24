use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::Result,
    routing::{get, post},
};
use futures::StreamExt;
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::net::TcpListener;

const ORDER_INBOUND_CHANNEL: &str = "order_inbound";
const ORDER_OUTBOUND_CHANNEL: &str = "order_outbound";

#[derive(Serialize, Deserialize, Clone)]
struct User {
    email: String,
    current_balance: i64,
    stocks: HashMap<String, u64>,
}

#[derive(Deserialize, Serialize, Debug)]
struct UserRequest {
    email: String,
}

#[derive(Serialize, Deserialize, Debug)]
struct Order {
    symbol: String,
    side: String,
    quantity: u32,
    price: Option<i64>,
    user: String,
}

#[derive(Clone)]
struct AppState {
    db: Db,
    redis_client: Client,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TradeEvent {
    pub buyer: String,
    pub seller: String,
    pub symbol: String,
    pub quantity: u64,
    pub price: i64,
}

type Db = Arc<Mutex<HashMap<String, User>>>;

#[tokio::main]
async fn main() {
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let redis_client = match redis::Client::open("redis://127.0.0.1/") {
        Ok(client) => client,
        Err(e) => {
            println!("Error connecting to the redis instance: {:#?}", e);
            return;
        }
    };

    let state = AppState {
        db: db.clone(),
        redis_client: redis_client.clone(),
    };

    // spawn background task to handle outbound events
    tokio::spawn(listen_outbound(redis_client.clone(), db.clone()));

    let app = Router::new()
        .route("/user", post(create_user))
        .route("/user/{email}", get(get_user))
        .route("/users", get(get_all_users))
        .route("/place_order", post(place_order))
        .with_state(state);

    let listener = TcpListener::bind("localhost:8080").await.unwrap();
    println!("🚀 Server running on http://localhost:8080");

    axum::serve(listener, app).await.unwrap();
}

// Create new user
async fn create_user(
    State(state): State<AppState>,
    Json(payload): Json<UserRequest>,
) -> Json<String> {
    let mut db = state.db.lock().unwrap();

    let user = User {
        email: payload.email.clone(),
        current_balance: 500000,
        stocks: HashMap::new(),
    };

    db.insert(payload.email.clone(), user.clone());
    Json(user.email)
}

// Fetch individual user
async fn get_user(state: State<AppState>, Path(email): Path<String>) -> Result<Json<User>> {
    let db = state.db.lock().unwrap();

    // Attempt to get the user from the database
    let user = db.get(&email).cloned();

    // Check if the user was found
    if let Some(user) = user {
        Ok(Json(user))
    } else {
        // If no user is found, return a 404 Not Found error
        Err(StatusCode::NOT_FOUND.into())
    }
}

// Fetch all users
async fn get_all_users(State(state): State<AppState>) -> Json<Vec<User>> {
    let db = state.db.lock().unwrap();
    let users = db.values().cloned().collect();
    Json(users)
}

async fn place_order(
    State(state): State<AppState>,
    Json(order): Json<Order>,
) -> Json<serde_json::Value> {
    // get a multiplexed async connection
    let mut conn = state
        .redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("failed to get Redis connection");

    // serialize order
    let payload = serde_json::to_string(&order).unwrap();

    // publish to redis channel
    let _: () = conn.publish(ORDER_INBOUND_CHANNEL, payload).await.unwrap();

    Json(serde_json::json!({
        "status": "submitted"
    }))
}

async fn listen_outbound(client: Client, db: Db) {
    // Get PubSub connection
    let mut pubsub = client
        .get_async_pubsub()
        .await
        .expect("failed to open PubSub connection");

    // Subscribe to outbound channel
    pubsub
        .subscribe(ORDER_OUTBOUND_CHANNEL)
        .await
        .expect("failed to subscribe");

    let mut stream = pubsub.on_message();

    println!(
        "📡 Listening for trade events on {}",
        ORDER_OUTBOUND_CHANNEL
    );

    while let Some(msg) = stream.next().await {
        let payload: String = match msg.get_payload() {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Failed to parse message: {:?}", e);
                continue;
            }
        };

        match serde_json::from_str::<TradeEvent>(&payload) {
            Ok(event) => {
                println!("Received trade event: {:?}", event);

                // Update user DB
                let mut db = db.lock().unwrap();
                if let Some(buyer) = db.get_mut(&event.buyer) {
                    // Buyer spends money
                    buyer.current_balance -= event.price * event.quantity as i64;
                    // Buyer gains stock
                    *buyer.stocks.entry(event.symbol.clone()).or_insert(0) += event.quantity;
                }
                if let Some(seller) = db.get_mut(&event.seller) {
                    // Seller receives money
                    seller.current_balance += event.price * event.quantity as i64;

                    // Seller loses stock, so subtract the quantity
                    if let Some(current_quantity) = seller.stocks.get_mut(&event.symbol) {
                        *current_quantity = current_quantity.saturating_sub(event.quantity as u64);
                    }
                }
            }
            Err(e) => {
                println!(
                    "Failed to deserialize TradeEvent: {:?}, raw: {}",
                    e, payload
                );
            }
        }
    }
}
