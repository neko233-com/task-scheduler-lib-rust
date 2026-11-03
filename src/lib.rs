//! task-scheduler-lib-rust — embeddable worker SDK for
//! [server-task-scheduler](https://github.com/neko233-com/server-task-scheduler).
//!
//! The worker auto-registers with the center, sends heartbeats, declares the
//! tasks it can run, and serves `POST /execute` so the center can dispatch
//! scheduled jobs to it. It is dependency-light enough to vendor into a project.

pub mod model;
pub use model::*;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::Value;

/// A task handler. Receives the schedule params and returns an output string or
/// an error string.
pub type TaskFn = Box<dyn Fn(Value) -> Result<String, String> + Send + Sync>;

/// Shared state injected into the `/execute` route.
#[derive(Clone)]
pub struct WorkerState {
    pub tasks: Arc<Mutex<HashMap<String, Arc<TaskFn>>>>,
    pub worker_id: Arc<Mutex<String>>,
}

/// An embeddable worker.
pub struct Worker {
    server_url: String,
    name: String,
    app: String,
    listen_addr: String,
    public_addr: String,
    tasks: Arc<Mutex<HashMap<String, Arc<TaskFn>>>>,
    worker_id: Arc<Mutex<String>>,
    client: reqwest::Client,
}

impl Worker {
    /// Construct a worker.
    ///
    /// * `server_url`  — center base, e.g. `"http://localhost:6432"`
    /// * `name`        — human-readable worker name
    /// * `app`         — application/group name
    /// * `listen_addr` — local bind, e.g. `"0.0.0.0:8082"`
    /// * `public_addr` — URL the center calls back, e.g. `"http://localhost:8082"`
    pub fn new(
        server_url: impl Into<String>,
        name: impl Into<String>,
        app: impl Into<String>,
        listen_addr: impl Into<String>,
        public_addr: impl Into<String>,
    ) -> Self {
        Worker {
            server_url: server_url.into(),
            name: name.into(),
            app: app.into(),
            listen_addr: listen_addr.into(),
            public_addr: public_addr.into(),
            tasks: Arc::new(Mutex::new(HashMap::new())),
            worker_id: Arc::new(Mutex::new(String::new())),
            client: reqwest::Client::new(),
        }
    }

    /// Register a handler for `task_key`. Call before `run`.
    pub fn handle<F>(&self, task_key: impl Into<String>, f: F)
    where
        F: Fn(Value) -> Result<String, String> + Send + Sync + 'static,
    {
        self.tasks
            .lock()
            .unwrap()
            .insert(task_key.into(), Arc::new(Box::new(f)));
    }

    fn task_keys(&self) -> Vec<String> {
        self.tasks.lock().unwrap().keys().cloned().collect()
    }

    async fn register(&self) -> Result<(String, i64), String> {
        let body = RegisterRequest {
            name: self.name.clone(),
            address: self.public_addr.clone(),
            app: self.app.clone(),
            tasks: self.task_keys(),
        };
        let resp = self
            .client
            .post(format!("{}/api/v1/workers/register", self.server_url))
            .json(&body)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if !resp.status().is_success() {
            return Err(format!("register status {}", resp.status()));
        }
        let r: RegisterResponse = resp.json().await.map_err(|e| e.to_string())?;
        Ok((r.worker_id, r.ttl_seconds))
    }

    /// Run the worker: register, heartbeat, and serve `/execute` until a
    /// shutdown signal (Ctrl-C). On exit it deregisters.
    pub async fn run(&self) -> Result<(), String> {
        let (worker_id, ttl) = self.register().await?;
        *self.worker_id.lock().unwrap() = worker_id.clone();
        let ttl_dur = if ttl > 0 {
            Duration::from_secs(ttl as u64)
        } else {
            Duration::from_secs(30)
        };
        println!(
            "[worker] registered id={} ttl={}s tasks={:?}",
            worker_id,
            ttl,
            self.task_keys()
        );

        // Heartbeat loop.
        let hb_client = self.client.clone();
        let hb_url = self.server_url.clone();
        let hb_tasks = self.tasks.clone();
        let hb_id = self.worker_id.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(ttl_dur / 2);
            loop {
                interval.tick().await;
                let id = hb_id.lock().unwrap().clone();
                if id.is_empty() {
                    continue;
                }
                let tasks = hb_tasks.lock().unwrap().keys().cloned().collect();
                let body = HeartbeatRequest {
                    worker_id: id,
                    tasks,
                };
                let _ = hb_client
                    .post(format!("{}/api/v1/workers/heartbeat", hb_url))
                    .json(&body)
                    .send()
                    .await;
            }
        });

        // HTTP server.
        let state = WorkerState {
            tasks: self.tasks.clone(),
            worker_id: self.worker_id.clone(),
        };
        let app = Router::new()
            .route("/execute", post(execute))
            .with_state(state);
        let listener = tokio::net::TcpListener::bind(&self.listen_addr)
            .await
            .map_err(|e| e.to_string())?;
        println!("[worker] serving /execute on {}", self.listen_addr);
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .map_err(|e| e.to_string())?;

        // Deregister on exit.
        let id = self.worker_id.lock().unwrap().clone();
        let _ = self
            .client
            .post(format!("{}/api/v1/workers/deregister", self.server_url))
            .json(&DeregisterRequest { worker_id: id })
            .send()
            .await;
        Ok(())
    }
}

async fn execute(
    State(state): State<WorkerState>,
    Json(req): Json<ExecuteRequest>,
) -> Json<ExecuteResponse> {
    let tasks = state.tasks.lock().unwrap();
    match tasks.get(&req.task_key) {
        Some(f) => match f(req.params) {
            Ok(out) => Json(ExecuteResponse {
                success: true,
                output: out,
                error: String::new(),
            }),
            Err(e) => Json(ExecuteResponse {
                success: false,
                output: String::new(),
                error: e,
            }),
        },
        None => Json(ExecuteResponse {
            success: false,
            output: String::new(),
            error: format!("unknown task_key {}", req.task_key),
        }),
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    println!("[worker] shutdown signal received");
}
