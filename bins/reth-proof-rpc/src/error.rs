//! Application error type for axum handlers.
//!
//! AppError converts into HTTP responses with appropriate status codes
//! and a consistent JSON error body { "error": "..." }.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

#[derive(Debug)]
pub enum AppError {
    /// The requested resource was not found (receipt not indexed, block
    /// non-canonical, address never touched, etc.).
    NotFound(String),

    /// The request was malformed (invalid address format, invalid block
    /// range, missing required field, etc.).
    BadRequest(String),

    /// An internal error occurred (DB query failed, MPT construction
    /// failed, etc.). The details are logged server-side; the client
    /// receives a generic message.
    Internal(eyre::Report),
}

impl AppError {
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::NotFound(msg.into())
    }

    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::BadRequest(msg.into())
    }
}

/// Convert any eyre::Report into an internal AppError.
impl From<eyre::Report> for AppError {
    fn from(e: eyre::Report) -> Self {
        Self::Internal(e)
    }
}

/// Convert AppError into an axum HTTP Response.
///
/// NotFound  → 404  { "error": "..." }
/// BadRequest → 400 { "error": "..." }
/// Internal   → 500 { "error": "internal server error" }  (details logged)
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        match self {
            AppError::NotFound(msg) => (
                StatusCode::NOT_FOUND,
                Json(json!({ "error": msg })),
            )
                .into_response(),

            AppError::BadRequest(msg) => (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": msg })),
            )
                .into_response(),

            AppError::Internal(report) => {
                // Log the full error chain server-side.
                // Never expose internal details to the client.
                tracing::error!("internal error: {:?}", report);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": "internal server error" })),
                )
                    .into_response()
            }
        }
    }
}