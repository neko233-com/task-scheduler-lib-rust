use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RegisterRequest {
    pub name: String,
    pub address: String,
    pub app: String,
    pub tasks: Vec<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct RegisterResponse {
    pub worker_id: String,
    pub ttl_seconds: i64,
}

#[derive(Serialize, Debug, Clone)]
pub struct HeartbeatRequest {
    pub worker_id: String,
    pub tasks: Vec<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct GenericAck {
    pub ok: bool,
    pub ttl_seconds: i64,
}

#[derive(Serialize, Debug, Clone)]
pub struct DeregisterRequest {
    pub worker_id: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ExecuteRequest {
    pub job_id: String,
    pub task_key: String,
    pub params: serde_json::Value,
    pub attempt: i64,
    pub timeout_seconds: i64,
}

#[derive(Serialize, Debug, Clone)]
pub struct ExecuteResponse {
    pub success: bool,
    pub output: String,
    pub error: String,
}
