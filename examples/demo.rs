//! Runnable demo worker for server-task-scheduler (Rust SDK).
//!
//! ```sh
//! SCHEDULER_URL=http://localhost:6432 cargo run --example demo
//! ```
//! Then create a schedule on the center with task_key "hello".

use serde_json::Value;
use task_scheduler_lib_rust::Worker;

#[tokio::main]
async fn main() {
    let server_url =
        std::env::var("SCHEDULER_URL").unwrap_or_else(|_| "http://localhost:6432".to_string());

    let worker = Worker::new(
        server_url,
        "demo-rust",
        "demo",
        "0.0.0.0:8082",
        "http://localhost:8082",
    );

    worker.handle("hello", |params: Value| {
        let name = params
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("world");
        let msg = format!("hello, {}! (worker=demo-rust)", name);
        println!("{}", msg);
        Ok(msg)
    });

    if let Err(e) = worker.run().await {
        eprintln!("worker error: {}", e);
        std::process::exit(1);
    }
}
