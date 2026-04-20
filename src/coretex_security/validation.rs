//! Input Validation - SQL/Injection Attack Protection for CoreTexDB

use std::collections::HashSet;

pub struct InputValidator;

impl InputValidator {
    pub fn new() -> Self {
        Self
    }

    pub fn validate_identifier(&self, input: &str) -> Result<(), String> {
        if input.is_empty() {
            return Err("Identifier cannot be empty".to_string());
        }
        
        let first_char = input.chars().next().unwrap();
        if !first_char.is_ascii_alphabetic() && first_char != '_' {
            return Err("Identifier must start with a letter or underscore".to_string());
        }
        
        for c in input.chars() {
            if !c.is_ascii_alphanumeric() && c != '_' {
                return Err(format!("Invalid character '{}' in identifier", c));
            }
        }
        
        Ok(())
    }

    pub fn sanitize_string(&self, input: &str) -> String {
        input
            .replace('\\', "\\\\")
            .replace('\'', "\\'")
            .replace('"', "\\\"")
            .replace('\x00', "\\0")
            .replace('\n', "\\n")
            .replace('\r', "\\r")
            .replace('\t', "\\t")
    }

    pub fn check_sql_injection(&self, input: &str) -> Result<(), String> {
        let dangerous_patterns = [
            "';", "--", "/*", "*/", 
            "xp_", "sp_", "exec", "execute",
            "union", "select", "insert", "update", "delete",
            "drop", "create", "alter", "truncate",
            "javascript:", "onerror=", "onload=",
            "<script", "</script>",
            "${", "#{", "{{",
        ];
        
        let lower = input.to_lowercase();
        
        for pattern in dangerous_patterns.iter() {
            if lower.contains(&pattern.to_lowercase()) {
                return Err(format!("Potential injection detected: contains '{}'", pattern));
            }
        }
        
        Ok(())
    }

    pub fn validate_collection_name(&self, name: &str) -> Result<(), String> {
        if name.is_empty() {
            return Err("Collection name cannot be empty".to_string());
        }
        
        if name.len() > 128 {
            return Err("Collection name too long (max 128 chars)".to_string());
        }
        
        self.validate_identifier(name)?;
        
        let reserved = ["system", "information_schema", "pg_catalog"];
        if reserved.contains(&name.to_lowercase().as_str()) {
            return Err("Reserved collection name".to_string());
        }
        
        Ok(())
    }

    pub fn validate_vector_id(&self, id: &str) -> Result<(), String> {
        if id.is_empty() {
            return Err("Vector ID cannot be empty".to_string());
        }
        
        if id.len() > 256 {
            return Err("Vector ID too long (max 256 chars)".to_string());
        }
        
        for c in id.chars() {
            if !c.is_ascii_alphanumeric() && c != '_' && c != '-' && c != '.' && c != ':' {
                return Err(format!("Invalid character '{}' in vector ID", c));
            }
        }
        
        Ok(())
    }

    pub fn validate_dimension(&self, dimension: usize) -> Result<(), String> {
        if dimension == 0 {
            return Err("Dimension must be greater than 0".to_string());
        }
        
        if dimension > 10000 {
            return Err("Dimension too large (max 10000)".to_string());
        }
        
        Ok(())
    }

    pub fn validate_limit(&self, limit: usize) -> Result<(), String> {
        if limit == 0 {
            return Err("Limit must be greater than 0".to_string());
        }
        
        if limit > 10000 {
            return Err("Limit too large (max 10000)".to_string());
        }
        
        Ok(())
    }

    pub fn sanitize_metadata(&self, metadata: &str) -> Result<serde_json::Value, String> {
        self.check_sql_injection(metadata)?;
        
        serde_json::from_str(metadata)
            .map_err(|e| format!("Invalid JSON: {}", e))
    }

    pub fn validate_ip_address(&self, ip: &str) -> Result<(), String> {
        let parts: Vec<&str> = ip.split('.').collect();
        
        if parts.len() != 4 {
            return Err("Invalid IP address format".to_string());
        }
        
        for part in parts {
            let num: u8 = part.parse()
                .map_err(|_| format!("Invalid IP address: {}", ip))?;
            
            if num > 255 {
                return Err("Invalid IP address octet".to_string());
            }
        }
        
        Ok(())
    }

    pub fn validate_email(&self, email: &str) -> Result<(), String> {
        let parts: Vec<&str> = email.split('@').collect();
        
        if parts.len() != 2 {
            return Err("Invalid email format".to_string());
        }
        
        if parts[0].is_empty() || parts[1].is_empty() {
            return Err("Invalid email format".to_string());
        }
        
        if !parts[1].contains('.') {
            return Err("Invalid email domain".to_string());
        }
        
        Ok(())
    }

    pub fn validate_password(&self, password: &str) -> Result<(), String> {
        if password.len() < 8 {
            return Err("Password must be at least 8 characters".to_string());
        }
        
        if password.len() > 128 {
            return Err("Password too long (max 128 chars)".to_string());
        }
        
        let has_upper = password.chars().any(|c| c.is_ascii_uppercase());
        let has_lower = password.chars().any(|c| c.is_ascii_lowercase());
        let has_digit = password.chars().any(|c| c.is_ascii_digit());
        
        if !has_upper || !has_lower || !has_digit {
            return Err("Password must contain uppercase, lowercase, and digits".to_string());
        }
        
        Ok(())
    }
}

impl Default for InputValidator {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RateLimitValidator {
    max_requests_per_minute: usize,
    blocked_ips: HashSet<String>,
}

impl RateLimitValidator {
    pub fn new(max_requests_per_minute: usize) -> Self {
        Self {
            max_requests_per_minute,
            blocked_ips: HashSet::new(),
        }
    }

    pub fn is_blocked(&self, ip: &str) -> bool {
        self.blocked_ips.contains(ip)
    }

    pub fn block_ip(&mut self, ip: String) {
        self.blocked_ips.insert(ip);
    }

    pub fn unblock_ip(&mut self, ip: &str) {
        self.blocked_ips.remove(ip);
    }

    pub fn validate_request(&mut self, ip: &str) -> Result<(), String> {
        if self.is_blocked(ip) {
            return Err("IP address is blocked".to_string());
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_identifier() {
        let validator = InputValidator::new();
        
        assert!(validator.validate_identifier("valid_name").is_ok());
        assert!(validator.validate_identifier("_private").is_ok());
        assert!(validator.validate_identifier("123invalid").is_err());
    }

    #[test]
    fn test_sanitize_string() {
        let validator = InputValidator::new();
        
        let sanitized = validator.sanitize_string("It's a test");
        assert!(!sanitized.contains('\''));
    }

    #[test]
    fn test_sql_injection_detection() {
        let validator = InputValidator::new();
        
        assert!(validator.check_sql_injection("normal text").is_ok());
        assert!(validator.check_sql_injection("'; DROP TABLE users--").is_err());
        assert!(validator.check_sql_injection("<script>alert(1)</script>").is_err());
    }

    #[test]
    fn test_validate_collection_name() {
        let validator = InputValidator::new();
        
        assert!(validator.validate_collection_name("my_collection").is_ok());
        assert!(validator.validate_collection_name("system").is_err());
    }

    #[test]
    fn test_validate_password() {
        let validator = InputValidator::new();
        
        assert!(validator.validate_password("Weak1").is_err());
        assert!(validator.validate_password("Str0ngP@ss").is_ok());
    }

    #[test]
    fn test_validate_ip() {
        let validator = InputValidator::new();
        
        assert!(validator.validate_ip_address("192.168.1.1").is_ok());
        assert!(validator.validate_ip_address("256.1.1.1").is_err());
    }

    #[test]
    fn test_rate_limit() {
        let mut validator = RateLimitValidator::new(100);
        
        assert!(validator.validate_request("192.168.1.1").is_ok());
        
        validator.block_ip("192.168.1.1".to_string());
        assert!(validator.validate_request("192.168.1.1").is_err());
    }
}
