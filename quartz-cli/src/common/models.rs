use axum::{
    Json, http::{self, StatusCode}, response::{IntoResponse, Response}
};
use serde::{Deserialize, Serialize};

pub struct ApiResponse;

impl ApiResponse {
    pub fn ok<T>(message: &str, data_opt: Option<T>) -> ApiOk<T> {
        ApiOk {
            message: message.to_string(),
            data: data_opt,
        }
    }

    pub fn error<'a>(status_code: http::StatusCode, error: String) -> ApiError {
        ApiError {
            status_code: status_code,
            error: error,
        }
    }
}


#[derive(Serialize, Deserialize)]
pub struct ApiOk<T> {
    pub message: String,
    pub data: Option<T>,
}

impl<'a, T: Serialize> IntoResponse for ApiOk<T> {
    fn into_response(self) -> Response {
        (StatusCode::OK, Json(self)).into_response()
    }
}

#[derive(Serialize, Deserialize)]
pub struct ApiError {
    #[serde(skip)]
    pub status_code: StatusCode,
    pub error: String,
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status_code, Json(self)).into_response()
    }
}

#[derive(Serialize, Deserialize)]
pub struct AppInfo {
    pub message: String,
    pub version: String,
    pub build_hash: String,
    pub build_time: String,
}

impl AppInfo {
    pub fn new() -> Self {
        AppInfo {
            message: "Welcome to QuartzDB!".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            build_hash: "unknown".to_string(), //env!("GIT_HASH").to_string(),
            build_time: "unknown".to_string(), //env!("BUILD_TIME").to_string(),
        }
    }
}
