#![forbid(unsafe_code)]
//! Transport-neutral S3MinIO SDK contracts plus HTTP and gRPC adapters.
//!
//! This ports the reusable `sdk/services/s3minio/helper` surface. Protobuf
//! generation remains in service-owned `pc-proto`; generated tonic clients
//! implement [`GrpcService`] without coupling this crate to one proto snapshot.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// gRPC OK code.
pub const CODE_OK: u32 = 0;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadRequest {
    pub object: String,
    pub path: String,
    pub bucket: String,
    pub expires: i32,
    pub user_id: i64,
    pub merchant_id: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DownloadResponse {
    pub code: u32,
    pub status: String,
    pub message: String,
    pub data: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadRequest {
    pub filename: String,
    pub size: u64,
    pub content_type: String,
    #[serde(skip)]
    pub content: Vec<u8>,
    pub bucket: String,
    pub path: String,
    pub expires: u32,
    pub user_id: i64,
    pub merchant_id: i64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadResult {
    pub filename: String,
    pub url: String,
    pub presigned_url: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct UploadResponse {
    pub code: u32,
    pub status: String,
    pub message: String,
    pub data: Option<UploadResult>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthResponse {
    pub code: u32,
    pub message: String,
    pub status: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReadyResponse {
    pub code: u32,
    pub message: String,
    pub status: String,
    pub dependencies: std::collections::BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FileDownloadResponse {
    pub code: u32,
    pub message: String,
    pub status: String,
    pub data: Vec<u8>,
    pub content_type: String,
    pub filename: String,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ViewResponse {
    pub code: u32,
    pub message: String,
    pub status: String,
    pub data: Vec<u8>,
    pub content_type: String,
}

#[derive(Debug, thiserror::Error)]
pub enum S3MinioError {
    #[error("s3minio {operation}: {message}")]
    Remote {
        operation: &'static str,
        message: String,
    },
    #[error("s3minio {0} response is missing")]
    Missing(&'static str),
    #[error("s3minio upload response data is missing")]
    MissingUploadData,
    #[error("invalid request: {0}")]
    InvalidRequest(&'static str),
    #[error("s3minio capability is unavailable over this transport: {0}")]
    Unsupported(&'static str),
    #[error("s3minio grpc: {0}")]
    Grpc(String),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl From<tonic::Status> for S3MinioError {
    fn from(status: tonic::Status) -> Self {
        Self::Grpc(status.to_string())
    }
}

/// Full service-scoped client capability.
#[async_trait]
pub trait Client: Send + Sync {
    async fn download(&self, request: &DownloadRequest) -> Result<DownloadResponse, S3MinioError>;
    async fn generate_view_url(
        &self,
        request: &DownloadRequest,
    ) -> Result<DownloadResponse, S3MinioError>;
    async fn upload(&self, request: &UploadRequest) -> Result<UploadResponse, S3MinioError>;
    async fn health(&self) -> Result<HealthResponse, S3MinioError>;
    async fn ready(&self) -> Result<ReadyResponse, S3MinioError>;
    async fn download_file(
        &self,
        request: &DownloadRequest,
    ) -> Result<FileDownloadResponse, S3MinioError>;
    async fn view(&self, path: &str) -> Result<ViewResponse, S3MinioError>;
}

/// Build the common download/view-url request.
#[must_use]
pub fn build_download_request(
    object: &str,
    user_id: i64,
    merchant_id: i64,
    path: Option<&str>,
    bucket: Option<&str>,
    expires: Option<i32>,
) -> DownloadRequest {
    DownloadRequest {
        object: object.to_string(),
        path: path
            .filter(|v| !v.is_empty())
            .unwrap_or_default()
            .to_string(),
        bucket: bucket
            .filter(|v| !v.is_empty())
            .unwrap_or_default()
            .to_string(),
        expires: expires.filter(|v| *v > 0).unwrap_or_default(),
        user_id,
        merchant_id,
    }
}

/// Read a file and build an upload request.
pub fn build_upload_request_for_file(
    user_id: i64,
    merchant_id: i64,
    path: &str,
    file_location: impl AsRef<Path>,
    bucket: Option<&str>,
    expires: Option<u32>,
) -> Result<UploadRequest, S3MinioError> {
    let location = file_location.as_ref();
    let content = std::fs::read(location)?;
    let filename = location
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or_default()
        .to_string();
    Ok(UploadRequest {
        filename,
        size: u64::try_from(content.len()).unwrap_or(u64::MAX),
        content_type: mime_from_extension(location),
        content,
        bucket: bucket
            .filter(|v| !v.is_empty())
            .unwrap_or_default()
            .to_string(),
        path: path.to_string(),
        expires: expires.filter(|v| *v > 0).unwrap_or_default(),
        user_id,
        merchant_id,
    })
}

/// Convenience facade mirroring the Go helper operations.
#[derive(Clone)]
pub struct S3Minio {
    client: Arc<dyn Client>,
}

impl S3Minio {
    #[must_use]
    pub fn new(client: Arc<dyn Client>) -> Self {
        Self { client }
    }

    pub async fn get_presigned_url(
        &self,
        request: &DownloadRequest,
    ) -> Result<String, S3MinioError> {
        let response = self.client.download(request).await?;
        ensure_ok("download", response.code, &response.message)?;
        Ok(if response.data.is_empty() {
            request.object.clone()
        } else {
            response.data
        })
    }

    pub async fn get_view_url(&self, request: &DownloadRequest) -> Result<String, S3MinioError> {
        let response = self.client.generate_view_url(request).await?;
        ensure_ok("view", response.code, &response.message)?;
        Ok(if response.data.is_empty() {
            request.object.clone()
        } else {
            response.data
        })
    }

    pub async fn upload(&self, request: &UploadRequest) -> Result<UploadResult, S3MinioError> {
        let response = self.client.upload(request).await?;
        ensure_ok("upload", response.code, &response.message)?;
        response.data.ok_or(S3MinioError::MissingUploadData)
    }

    pub async fn health(&self) -> Result<HealthResponse, S3MinioError> {
        let response = self.client.health().await?;
        ensure_ok("health", response.code, &response.message)?;
        Ok(response)
    }

    pub async fn ready(&self) -> Result<ReadyResponse, S3MinioError> {
        let response = self.client.ready().await?;
        ensure_ok("ready", response.code, &response.message)?;
        Ok(response)
    }
}

fn ensure_ok(operation: &'static str, code: u32, message: &str) -> Result<(), S3MinioError> {
    if code == CODE_OK {
        Ok(())
    } else {
        Err(S3MinioError::Remote {
            operation,
            message: if message.is_empty() {
                format!("{operation} failed")
            } else {
                message.to_string()
            },
        })
    }
}

/// RPC surface implemented by a thin wrapper around a service-owned generated
/// tonic client.
///
/// `pc-proto` deliberately remains outside this reusable repository. This
/// boundary keeps protobuf ownership with the service while the request,
/// response, readiness, and unsupported-operation behavior stays shared.
#[async_trait]
pub trait GrpcService: Send + Sync {
    async fn download(&self, request: DownloadRequest) -> Result<DownloadResponse, tonic::Status>;
    async fn generate_view_url(
        &self,
        request: DownloadRequest,
    ) -> Result<DownloadResponse, tonic::Status>;
    async fn upload(&self, request: UploadRequest) -> Result<UploadResponse, tonic::Status>;
    async fn health(&self) -> Result<String, tonic::Status>;
}

/// S3MinIO gRPC facade backed by a service-owned [`GrpcService`] adapter.
#[derive(Clone)]
pub struct GrpcClient<T> {
    service: T,
}

impl<T> GrpcClient<T> {
    #[must_use]
    pub const fn new(service: T) -> Self {
        Self { service }
    }

    #[must_use]
    pub const fn service(&self) -> &T {
        &self.service
    }
}

#[async_trait]
impl<T> Client for GrpcClient<T>
where
    T: GrpcService,
{
    async fn download(&self, request: &DownloadRequest) -> Result<DownloadResponse, S3MinioError> {
        Ok(self.service.download(request.clone()).await?)
    }

    async fn generate_view_url(
        &self,
        request: &DownloadRequest,
    ) -> Result<DownloadResponse, S3MinioError> {
        Ok(self.service.generate_view_url(request.clone()).await?)
    }

    async fn upload(&self, request: &UploadRequest) -> Result<UploadResponse, S3MinioError> {
        Ok(self.service.upload(request.clone()).await?)
    }

    async fn health(&self) -> Result<HealthResponse, S3MinioError> {
        let raw = self.service.health().await?;
        let trimmed = raw.trim();
        let status = if trimmed.eq_ignore_ascii_case("ok") {
            "ok"
        } else {
            trimmed
        };
        let code = if status == "ok" { CODE_OK } else { 503 };
        Ok(HealthResponse {
            code,
            status: status.to_string(),
            message: "grpc health".to_string(),
        })
    }

    async fn ready(&self) -> Result<ReadyResponse, S3MinioError> {
        let health = self.health().await?;
        let status = if health.status.is_empty() {
            "unavailable".to_string()
        } else {
            health.status
        };
        Ok(ReadyResponse {
            code: health.code,
            message: "grpc readiness".to_string(),
            dependencies: [("grpc".to_string(), status.clone())].into(),
            status,
        })
    }

    async fn download_file(
        &self,
        _request: &DownloadRequest,
    ) -> Result<FileDownloadResponse, S3MinioError> {
        Err(S3MinioError::Unsupported(
            "download_file is not exposed over s3minio grpc yet",
        ))
    }

    async fn view(&self, _path: &str) -> Result<ViewResponse, S3MinioError> {
        Err(S3MinioError::Unsupported(
            "view stream is not exposed over s3minio grpc yet",
        ))
    }
}

/// HTTP bridge adapter for `/api/v2/*`, `/healthz`, and `/readyz`.
#[derive(Clone)]
pub struct HttpClient {
    base_url: Arc<str>,
    client: reqwest::Client,
}

impl HttpClient {
    pub fn new(base_url: &str, client: Option<reqwest::Client>) -> Result<Self, S3MinioError> {
        let client = match client {
            Some(client) => client,
            None => reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .build()?,
        };
        Ok(Self {
            base_url: Arc::from(base_url.trim_end_matches('/')),
            client,
        })
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    async fn download_like(
        &self,
        endpoint: &str,
        request: &DownloadRequest,
    ) -> Result<DownloadResponse, S3MinioError> {
        let response = self
            .client
            .post(self.url(endpoint))
            .json(request)
            .send()
            .await?;
        let status = response.status();
        let envelope = response.json::<Envelope<serde_json::Value>>().await?;
        if !status.is_success() {
            return Err(S3MinioError::Remote {
                operation: "download",
                message: if envelope.message.is_empty() {
                    status.to_string()
                } else {
                    envelope.message
                },
            });
        }
        let data = envelope
            .data
            .and_then(|value| value.as_str().map(ToOwned::to_owned))
            .unwrap_or_default();
        Ok(DownloadResponse {
            code: envelope.code,
            status: envelope.status,
            message: envelope.message,
            data,
        })
    }
}

#[derive(Deserialize)]
struct Envelope<T> {
    #[serde(default)]
    code: u32,
    #[serde(default)]
    status: String,
    #[serde(default)]
    message: String,
    data: Option<T>,
}

#[async_trait]
impl Client for HttpClient {
    async fn download(&self, request: &DownloadRequest) -> Result<DownloadResponse, S3MinioError> {
        self.download_like("/api/v2/download", request).await
    }

    async fn generate_view_url(
        &self,
        request: &DownloadRequest,
    ) -> Result<DownloadResponse, S3MinioError> {
        self.download_like("/api/v2/generate_view_url", request)
            .await
    }

    async fn upload(&self, request: &UploadRequest) -> Result<UploadResponse, S3MinioError> {
        if request.filename.is_empty() {
            return Err(S3MinioError::InvalidRequest("upload filename is required"));
        }
        let mut form = reqwest::multipart::Form::new()
            .part(
                "object",
                reqwest::multipart::Part::bytes(request.content.clone())
                    .file_name(request.filename.clone())
                    .mime_str(if request.content_type.is_empty() {
                        "application/octet-stream"
                    } else {
                        &request.content_type
                    })?,
            )
            .text("signed", "true");
        if !request.path.is_empty() {
            form = form.text("path", request.path.clone());
        }
        if !request.bucket.is_empty() {
            form = form.text("bucket", request.bucket.clone());
        }
        if request.expires > 0 {
            form = form.text("expires", request.expires.to_string());
        }
        let response = self
            .client
            .post(self.url("/api/v2/upload"))
            .multipart(form)
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(S3MinioError::Remote {
                operation: "upload",
                message: status.to_string(),
            });
        }
        let envelope = response.json::<Envelope<UploadResult>>().await?;
        Ok(UploadResponse {
            code: envelope.code,
            status: envelope.status,
            message: envelope.message,
            data: envelope.data,
        })
    }

    async fn health(&self) -> Result<HealthResponse, S3MinioError> {
        let response = self.client.get(self.url("/healthz")).send().await?;
        if !response.status().is_success() {
            return Err(S3MinioError::Remote {
                operation: "health",
                message: response.status().to_string(),
            });
        }
        Ok(HealthResponse {
            code: CODE_OK,
            status: "ok".to_string(),
            message: "http health".to_string(),
        })
    }

    async fn ready(&self) -> Result<ReadyResponse, S3MinioError> {
        let response = self.client.get(self.url("/readyz")).send().await?;
        let status_code = response.status();
        let status = if status_code.is_success() {
            "ok"
        } else {
            "unavailable"
        };
        Ok(ReadyResponse {
            code: u32::from(status_code.as_u16()),
            status: status.to_string(),
            message: "http ready".to_string(),
            dependencies: [("http".to_string(), status.to_string())].into(),
        })
    }

    async fn download_file(
        &self,
        request: &DownloadRequest,
    ) -> Result<FileDownloadResponse, S3MinioError> {
        let response = self
            .client
            .post(self.url("/api/v2/download_file"))
            .json(request)
            .send()
            .await?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        if !status.is_success() {
            return Err(S3MinioError::Remote {
                operation: "download file",
                message: status.to_string(),
            });
        }
        let data = response.bytes().await?.to_vec();
        Ok(FileDownloadResponse {
            code: u32::from(status.as_u16()),
            status: status.to_string(),
            message: "download file".to_string(),
            data,
            content_type,
            filename: String::new(),
        })
    }

    async fn view(&self, path: &str) -> Result<ViewResponse, S3MinioError> {
        if path.is_empty() {
            return Err(S3MinioError::InvalidRequest("view path is required"));
        }
        let response = self
            .client
            .get(self.url("/api/v2/view"))
            .query(&[("path", path)])
            .send()
            .await?;
        let status = response.status();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        if !status.is_success() {
            return Err(S3MinioError::Remote {
                operation: "view",
                message: status.to_string(),
            });
        }
        let data = response.bytes().await?.to_vec();
        Ok(ViewResponse {
            code: u32::from(status.as_u16()),
            status: status.to_string(),
            message: "view".to_string(),
            data,
            content_type,
        })
    }
}

fn mime_from_extension(path: &Path) -> String {
    match path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "json" => "application/json",
        "pdf" => "application/pdf",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "csv" => "text/csv",
        "txt" => "text/plain",
        _ => "application/octet-stream",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone)]
    struct FakeGrpc {
        health: &'static str,
    }

    #[async_trait]
    impl GrpcService for FakeGrpc {
        async fn download(
            &self,
            request: DownloadRequest,
        ) -> Result<DownloadResponse, tonic::Status> {
            Ok(DownloadResponse {
                code: CODE_OK,
                data: request.object,
                ..DownloadResponse::default()
            })
        }

        async fn generate_view_url(
            &self,
            request: DownloadRequest,
        ) -> Result<DownloadResponse, tonic::Status> {
            self.download(request).await
        }

        async fn upload(&self, request: UploadRequest) -> Result<UploadResponse, tonic::Status> {
            Ok(UploadResponse {
                code: CODE_OK,
                data: Some(UploadResult {
                    filename: request.filename,
                    ..UploadResult::default()
                }),
                ..UploadResponse::default()
            })
        }

        async fn health(&self) -> Result<String, tonic::Status> {
            Ok(self.health.to_string())
        }
    }

    #[test]
    fn download_builder_applies_optional_values() {
        let request = build_download_request("obj", 7, 8, Some("path"), Some("bucket"), Some(60));
        assert_eq!(request.object, "obj");
        assert_eq!(request.path, "path");
        assert_eq!(request.bucket, "bucket");
        assert_eq!(request.expires, 60);
    }

    #[test]
    fn download_builder_ignores_non_positive_expiry() {
        assert_eq!(
            build_download_request("obj", 0, 0, None, None, Some(-1)).expires,
            0
        );
    }

    #[test]
    fn code_ok_matches_grpc() {
        assert_eq!(CODE_OK, 0);
        assert!(ensure_ok("download", CODE_OK, "").is_ok());
        assert!(ensure_ok("download", 13, "failed").is_err());
    }

    #[test]
    fn mime_mapping_has_safe_default() {
        assert_eq!(mime_from_extension(Path::new("x.pdf")), "application/pdf");
        assert_eq!(
            mime_from_extension(Path::new("x.unknown")),
            "application/octet-stream"
        );
    }

    #[tokio::test]
    async fn grpc_facade_maps_health_and_unsupported_streams() {
        let client = GrpcClient::new(FakeGrpc { health: " OK " });
        let health = client.health().await.unwrap();
        assert_eq!(health.code, CODE_OK);
        assert_eq!(health.status, "ok");

        let ready = client.ready().await.unwrap();
        assert_eq!(
            ready.dependencies.get("grpc").map(String::as_str),
            Some("ok")
        );
        assert!(matches!(
            client.view("/object").await,
            Err(S3MinioError::Unsupported(_))
        ));
    }

    #[tokio::test]
    async fn grpc_facade_preserves_request_mapping() {
        let client = GrpcClient::new(FakeGrpc { health: "down" });
        let request = build_download_request("receipt.pdf", 7, 8, None, None, None);
        assert_eq!(client.download(&request).await.unwrap().data, "receipt.pdf");
        assert_eq!(client.ready().await.unwrap().code, 503);
    }
}
