//! HTTP-based S3/MinIO storage backend
//! Does not require AWS SDK, uses raw HTTP requests with AWS Signature V4

use std::time::{SystemTime, UNIX_EPOCH};
use hmac::{Hmac, Mac};
use sha2::{Sha256, Digest};

use super::StorageBackendTrait;

type HmacSha256 = Hmac<Sha256>;

/// HTTP-based S3-compatible storage client
/// Works with AWS S3, MinIO, and any S3-compatible service
pub struct HttpS3Storage {
    config: S3ConfigInner,
    client: reqwest::Client,
}

struct S3ConfigInner {
    bucket: String,
    region: String,
    access_key: String,
    secret_key: String,
    endpoint: String,
    use_ssl: bool,
}

impl HttpS3Storage {
    pub fn new(
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        endpoint: Option<&str>,
        use_ssl: bool,
    ) -> Self {
        let default_endpoint = format!("s3.{}.amazonaws.com", region);
        let ep = endpoint.unwrap_or(&default_endpoint);

        Self {
            config: S3ConfigInner {
                bucket: bucket.to_string(),
                region: region.to_string(),
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                endpoint: ep.to_string(),
                use_ssl,
            },
            client: reqwest::Client::new(),
        }
    }

    /// Create MinIO storage client
    pub fn new_minio(
        bucket: &str,
        endpoint: &str,
        access_key: &str,
        secret_key: &str,
        use_ssl: bool,
    ) -> Self {
        Self {
            config: S3ConfigInner {
                bucket: bucket.to_string(),
                region: "us-east-1".to_string(),
                access_key: access_key.to_string(),
                secret_key: secret_key.to_string(),
                endpoint: endpoint.to_string(),
                use_ssl,
            },
            client: reqwest::Client::new(),
        }
    }

    fn base_url(&self) -> String {
        let scheme = if self.config.use_ssl { "https" } else { "http" };
        if self.config.endpoint.starts_with("http://") || self.config.endpoint.starts_with("https://") {
            format!("{}/{}", self.config.endpoint.trim_end_matches('/'), self.config.bucket)
        } else {
            format!("{}://{}/{}", scheme, self.config.endpoint.trim_end_matches('/'), self.config.bucket)
        }
    }

    fn host(&self) -> String {
        if self.config.endpoint.starts_with("http://") {
            self.config.endpoint.strip_prefix("http://").unwrap_or(&self.config.endpoint).trim_end_matches('/').to_string()
        } else if self.config.endpoint.starts_with("https://") {
            self.config.endpoint.strip_prefix("https://").unwrap_or(&self.config.endpoint).trim_end_matches('/').to_string()
        } else {
            self.config.endpoint.trim_end_matches('/').to_string()
        }
    }

    /// Generate AWS Signature V4 authorization header
    fn sign_request(
        &self,
        method: &str,
        path: &str,
        date: &str,
        amz_date: &str,
        content_sha256: &str,
        _payload: &[u8],
    ) -> String {
        // Step 1: Create canonical request
        let canonical_uri = if path.is_empty() { "/" } else { path };
        let canonical_querystring = "";

        let headers = format!(
            "content-sha256:{}\nhost:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
            content_sha256,
            self.host(),
            content_sha256,
            amz_date
        );

        let signed_headers = "content-sha256;host;x-amz-content-sha256;x-amz-date";

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method,
            canonical_uri,
            canonical_querystring,
            headers,
            signed_headers,
            content_sha256
        );

        // Step 2: Create string to sign
        let credential_scope = format!("{}/{}/s3/aws4_request", date, self.config.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date,
            credential_scope,
            hex_hash(&canonical_request.as_bytes())
        );

        // Step 3: Calculate signature
        let signing_key = self.get_signing_key(date);
        let signature = hmac_sha256_hex(&signing_key, string_to_sign.as_bytes());

        // Step 4: Build authorization header
        format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.config.access_key,
            credential_scope,
            signed_headers,
            signature
        )
    }

    fn get_signing_key(&self, date: &str) -> Vec<u8> {
        let k_date = hmac_sha256(
            format!("AWS4{}", self.config.secret_key).as_bytes(),
            date.as_bytes(),
        );
        let k_region = hmac_sha256(&k_date, self.config.region.as_bytes());
        let k_service = hmac_sha256(&k_region, b"s3");
        hmac_sha256(&k_service, b"aws4_request")
    }
}

impl StorageBackendTrait for HttpS3Storage {
    fn write(&self, key: &str, data: &[u8]) -> Result<(), String> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        rt.block_on(self.write_async(key, data))
    }

    fn read(&self, key: &str) -> Result<Vec<u8>, String> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        rt.block_on(self.read_async(key))
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        rt.block_on(self.delete_async(key))
    }

    fn exists(&self, key: &str) -> bool {
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string());
        match rt {
            Ok(rt) => rt.block_on(self.exists_async(key)),
            Err(_) => false,
        }
    }

    fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
        rt.block_on(self.list_async(prefix))
    }
}

impl HttpS3Storage {
    async fn write_async(&self, key: &str, data: &[u8]) -> Result<(), String> {
        let url = format!("{}/{}", self.base_url(), key);
        let _now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let date = format!("{}", chrono::Utc::now().format("%Y%m%d"));
        let amz_date = format!("{}T{}Z", chrono::Utc::now().format("%Y%m%d"), chrono::Utc::now().format("%H%M%S"));
        let content_sha256 = hex_hash(data);

        let auth = self.sign_request("PUT", &format!("/{}", key), &date, &amz_date, &content_sha256, data);

        let response = self.client.put(&url)
            .header("Content-Type", "application/octet-stream")
            .header("x-amz-date", &amz_date)
            .header("x-amz-content-sha256", &content_sha256)
            .header("Authorization", &auth)
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| format!("S3 PUT error: {}", e))?;

        if response.status().is_success() {
            Ok(())
        } else {
            Err(format!("S3 PUT failed with status: {}", response.status()))
        }
    }

    async fn read_async(&self, key: &str) -> Result<Vec<u8>, String> {
        let url = format!("{}/{}", self.base_url(), key);
        let _now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let date = format!("{}", chrono::Utc::now().format("%Y%m%d"));
        let amz_date = format!("{}T{}Z", chrono::Utc::now().format("%Y%m%d"), chrono::Utc::now().format("%H%M%S"));
        let empty_sha = hex_hash(b"");

        let auth = self.sign_request("GET", &format!("/{}", key), &date, &amz_date, &empty_sha, b"");

        let response = self.client.get(&url)
            .header("x-amz-date", &amz_date)
            .header("x-amz-content-sha256", &empty_sha)
            .header("Authorization", &auth)
            .send()
            .await
            .map_err(|e| format!("S3 GET error: {}", e))?;

        if response.status().is_success() {
            response.bytes().await
                .map(|b| b.to_vec())
                .map_err(|e| format!("S3 GET body error: {}", e))
        } else {
            Err(format!("S3 GET failed with status: {}", response.status()))
        }
    }

    async fn delete_async(&self, key: &str) -> Result<(), String> {
        let url = format!("{}/{}", self.base_url(), key);
        let date = format!("{}", chrono::Utc::now().format("%Y%m%d"));
        let amz_date = format!("{}T{}Z", chrono::Utc::now().format("%Y%m%d"), chrono::Utc::now().format("%H%M%S"));
        let empty_sha = hex_hash(b"");

        let auth = self.sign_request("DELETE", &format!("/{}", key), &date, &amz_date, &empty_sha, b"");

        let response = self.client.delete(&url)
            .header("x-amz-date", &amz_date)
            .header("x-amz-content-sha256", &empty_sha)
            .header("Authorization", &auth)
            .send()
            .await
            .map_err(|e| format!("S3 DELETE error: {}", e))?;

        if response.status().is_success() || response.status().as_u16() == 404 {
            Ok(())
        } else {
            Err(format!("S3 DELETE failed with status: {}", response.status()))
        }
    }

    async fn exists_async(&self, key: &str) -> bool {
        let url = format!("{}/{}", self.base_url(), key);
        let date = format!("{}", chrono::Utc::now().format("%Y%m%d"));
        let amz_date = format!("{}T{}Z", chrono::Utc::now().format("%Y%m%d"), chrono::Utc::now().format("%H%M%S"));
        let empty_sha = hex_hash(b"");

        let auth = self.sign_request("HEAD", &format!("/{}", key), &date, &amz_date, &empty_sha, b"");

        self.client.head(&url)
            .header("x-amz-date", &amz_date)
            .header("x-amz-content-sha256", &empty_sha)
            .header("Authorization", &auth)
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    }

    async fn list_async(&self, prefix: &str) -> Result<Vec<String>, String> {
        let url = format!("{}/?prefix={}", self.base_url(), prefix);
        let date = format!("{}", chrono::Utc::now().format("%Y%m%d"));
        let amz_date = format!("{}T{}Z", chrono::Utc::now().format("%Y%m%d"), chrono::Utc::now().format("%H%M%S"));
        let empty_sha = hex_hash(b"");

        let auth = self.sign_request("GET", &format!("/?prefix={}", prefix), &date, &amz_date, &empty_sha, b"");

        let response = self.client.get(&url)
            .header("x-amz-date", &amz_date)
            .header("x-amz-content-sha256", &empty_sha)
            .header("Authorization", &auth)
            .send()
            .await
            .map_err(|e| format!("S3 LIST error: {}", e))?;

        if !response.status().is_success() {
            return Err(format!("S3 LIST failed with status: {}", response.status()));
        }

        let body = response.text().await
            .map_err(|e| format!("S3 LIST body error: {}", e))?;

        // Simple XML parsing for list of keys
        let mut keys = Vec::new();
        for line in body.lines() {
            let line = line.trim();
            if line.contains("<Key>") && line.contains("</Key>") {
                if let Some(start) = line.find("<Key>") {
                    if let Some(end) = line.find("</Key>") {
                        let key = &line[start + 5..end];
                        keys.push(key.to_string());
                    }
                }
            }
        }

        Ok(keys)
    }
}

// ═══════════════════════════════════════════════════════════════
// Cryptographic helpers
// ═══════════════════════════════════════════════════════════════

fn hex_hash(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    format!("{:x}", hasher.finalize())
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

fn hmac_sha256_hex(key: &[u8], data: &[u8]) -> String {
    let result = hmac_sha256(key, data);
    result.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_s3_storage_creation() {
        let storage = HttpS3Storage::new(
            "my-bucket",
            "us-east-1",
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            None,
            true,
        );
        assert_eq!(storage.config.bucket, "my-bucket");
    }

    #[test]
    fn test_minio_storage_creation() {
        let storage = HttpS3Storage::new_minio(
            "my-bucket",
            "localhost:9000",
            "minioadmin",
            "minioadmin",
            false,
        );
        assert_eq!(storage.config.endpoint, "localhost:9000");
        assert!(!storage.config.use_ssl);
    }

    #[test]
    fn test_hex_hash() {
        let hash = hex_hash(b"hello");
        assert_eq!(hash.len(), 64);
    }
}
