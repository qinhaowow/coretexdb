//! Network Isolation - IP Whitelist/Blacklist for CoreTexDB

use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use std::net::IpAddr;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkPolicy {
    pub id: String,
    pub name: String,
    pub whitelist: Vec<IpRange>,
    pub blacklist: Vec<IpRange>,
    pub default_action: PolicyAction,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IpRange {
    pub start: String,
    pub end: Option<String>,
    pub cidr: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyAction {
    Allow,
    Deny,
}

pub struct NetworkIsolation {
    policies: Arc<RwLock<HashSet<String>>>,
    whitelist: Arc<RwLock<HashSet<String>>>,
    blacklist: Arc<RwLock<HashSet<String>>>,
    default_action: PolicyAction,
    log_blocked: bool,
}

impl NetworkIsolation {
    pub fn new() -> Self {
        Self {
            policies: Arc::new(RwLock::new(HashSet::new())),
            whitelist: Arc::new(RwLock::new(HashSet::new())),
            blacklist: Arc::new(RwLock::new(HashSet::new())),
            default_action: PolicyAction::Deny,
            log_blocked: true,
        }
    }

    pub fn with_default_allow(mut self) -> Self {
        self.default_action = PolicyAction::Allow;
        self
    }

    pub fn with_logging(mut self, enabled: bool) -> Self {
        self.log_blocked = enabled;
        self
    }

    pub async fn add_to_whitelist(&self, ip: &str) -> Result<(), String> {
        if !Self::is_valid_ip(ip) {
            return Err(format!("Invalid IP address: {}", ip));
        }
        
        let mut whitelist = self.whitelist.write().await;
        whitelist.insert(ip.to_string());
        
        let mut blacklist = self.blacklist.write().await;
        blacklist.remove(ip);
        
        Ok(())
    }

    pub async fn add_to_blacklist(&self, ip: &str) -> Result<(), String> {
        if !Self::is_valid_ip(ip) {
            return Err(format!("Invalid IP address: {}", ip));
        }
        
        let mut blacklist = self.blacklist.write().await;
        blacklist.insert(ip.to_string());
        
        let mut whitelist = self.whitelist.write().await;
        whitelist.remove(ip);
        
        Ok(())
    }

    pub async fn remove_from_whitelist(&self, ip: &str) -> bool {
        let mut whitelist = self.whitelist.write().await;
        whitelist.remove(ip)
    }

    pub async fn remove_from_blacklist(&self, ip: &str) -> bool {
        let mut blacklist = self.blacklist.write().await;
        blacklist.remove(ip)
    }

    pub async fn check_access(&self, ip: &str) -> PolicyAction {
        if !Self::is_valid_ip(ip) {
            return PolicyAction::Deny;
        }
        
        let blacklist = self.blacklist.read().await;
        if blacklist.contains(ip) {
            if self.log_blocked {
                eprintln!("Blocked access from blacklisted IP: {}", ip);
            }
            return PolicyAction::Deny;
        }
        
        let whitelist = self.whitelist.read().await;
        if whitelist.contains(ip) {
            return PolicyAction::Allow;
        }
        
        self.default_action
    }

    pub async fn is_allowed(&self, ip: &str) -> bool {
        self.check_access(ip).await == PolicyAction::Allow
    }

    pub async fn is_blocked(&self, ip: &str) -> bool {
        self.check_access(ip).await == PolicyAction::Deny
    }

    pub async fn list_whitelist(&self) -> Vec<String> {
        let whitelist = self.whitelist.read().await;
        whitelist.iter().cloned().collect()
    }

    pub async fn list_blacklist(&self) -> Vec<String> {
        let blacklist = self.blacklist.read().await;
        blacklist.iter().cloned().collect()
    }

    pub async fn clear_whitelist(&self) {
        let mut whitelist = self.whitelist.write().await;
        whitelist.clear();
    }

    pub async fn clear_blacklist(&self) {
        let mut blacklist = self.blacklist.write().await;
        blacklist.clear();
    }

    pub async fn add_ip_range_to_whitelist(&self, start: &str, end: Option<&str>) -> Result<usize, String> {
        if !Self::is_valid_ip(start) {
            return Err(format!("Invalid IP address: {}", start));
        }
        
        let start_num = Self::ip_to_number(start);
        let end_num = end
            .map(|e| {
                if Self::is_valid_ip(e) {
                    Ok(Self::ip_to_number(e))
                } else {
                    Err(format!("Invalid IP: {}", e))
                }
            })
            .unwrap_or(Ok(start_num))?;
        
        let count = (end_num - start_num + 1) as usize;
        
        let mut whitelist = self.whitelist.write().await;
        
        for ip_num in start_num..=end_num {
            let ip = Self::number_to_ip(ip_num);
            whitelist.insert(ip);
        }
        
        Ok(count)
    }

    pub async fn add_cidr_to_whitelist(&self, cidr: &str) -> Result<usize, String> {
        let parts: Vec<&str> = cidr.split('/').collect();
        
        if parts.len() != 2 {
            return Err("Invalid CIDR format (use: x.x.x.x/nn)".to_string());
        }
        
        let prefix: u8 = parts[1].parse()
            .map_err(|_| "Invalid CIDR prefix".to_string())?;
        
        if prefix > 32 {
            return Err("CIDR prefix must be 0-32".to_string());
        }
        
        if !Self::is_valid_ip(parts[0]) {
            return Err(format!("Invalid IP: {}", parts[0]));
        }
        
        let base = Self::ip_to_number(parts[0]);
        let mask = !((1u32 << (32 - prefix)) - 1);
        let network = base & mask;
        let broadcast = network | !mask;
        
        let count = (1u32 << (32 - prefix)) as usize;
        
        let mut whitelist = self.whitelist.write().await;
        
        for ip_num in network..=broadcast {
            let ip = Self::number_to_ip(ip_num);
            whitelist.insert(ip);
        }
        
        Ok(count)
    }

    fn is_valid_ip(ip: &str) -> bool {
        ip.parse::<IpAddr>().is_ok()
    }

    fn ip_to_number(ip: &str) -> u32 {
        let parts: Vec<u8> = ip.split('.')
            .filter_map(|s| s.parse().ok())
            .collect();
        
        if parts.len() == 4 {
            ((parts[0] as u32) << 24)
                | ((parts[1] as u32) << 16)
                | ((parts[2] as u32) << 8)
                | (parts[3] as u32)
        } else {
            0
        }
    }

    fn number_to_ip(num: u32) -> String {
        format!(
            "{}.{}.{}.{}",
            (num >> 24) & 0xFF,
            (num >> 16) & 0xFF,
            (num >> 8) & 0xFF,
            num & 0xFF
        )
    }
}

impl Default for NetworkIsolation {
    fn default() -> Self {
        Self::new()
    }
}

pub struct IPRangeManager;

impl IPRangeManager {
    pub fn parse_range(start: &str, end: Option<&str>) -> Result<Vec<String>, String> {
        if !NetworkIsolation::is_valid_ip(start) {
            return Err(format!("Invalid IP: {}", start));
        }
        
        let start_num = NetworkIsolation::ip_to_number(start);
        
        let end_num = match end {
            Some(e) => {
                if !NetworkIsolation::is_valid_ip(e) {
                    return Err(format!("Invalid IP: {}", e));
                }
                NetworkIsolation::ip_to_number(e)
            }
            None => start_num,
        };
        
        if end_num < start_num {
            return Err("End IP must be >= start IP".to_string());
        }
        
        let mut ips = Vec::new();
        for ip_num in start_num..=end_num {
            ips.push(NetworkIsolation::number_to_ip(ip_num));
        }
        
        Ok(ips)
    }

    pub fn is_in_range(ip: &str, start: &str, end: &str) -> bool {
        let ip_num = NetworkIsolation::ip_to_number(ip);
        let start_num = NetworkIsolation::ip_to_number(start);
        let end_num = NetworkIsolation::ip_to_number(end);
        
        ip_num >= start_num && ip_num <= end_num
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_whitelist() {
        let isolation = NetworkIsolation::new();
        
        isolation.add_to_whitelist("192.168.1.1").await.unwrap();
        
        assert!(isolation.is_allowed("192.168.1.1").await);
        assert!(!isolation.is_allowed("10.0.0.1").await);
    }

    #[tokio::test]
    async fn test_blacklist() {
        let isolation = NetworkIsolation::new();
        
        isolation.add_to_blacklist("192.168.1.100").await.unwrap();
        
        assert!(isolation.is_blocked("192.168.1.100").await);
    }

    #[tokio::test]
    async fn test_whitelist_overrides_blacklist() {
        let isolation = NetworkIsolation::new();
        
        isolation.add_to_blacklist("192.168.1.1").await.unwrap();
        isolation.add_to_whitelist("192.168.1.1").await.unwrap();
        
        assert!(isolation.is_allowed("192.168.1.1").await);
    }

    #[tokio::test]
    async fn test_ip_range() {
        let isolation = NetworkIsolation::new();
        
        let count = isolation.add_ip_range_to_whitelist("192.168.1.0", Some("192.168.1.3")).await.unwrap();
        
        assert_eq!(count, 4);
        assert!(isolation.is_allowed("192.168.1.2").await);
    }

    #[test]
    fn test_ip_range_manager() {
        let ips = IPRangeManager::parse_range("10.0.0.1", Some("10.0.0.3")).unwrap();
        
        assert_eq!(ips.len(), 3);
        assert!(ips.contains(&"10.0.0.2".to_string()));
    }

    #[test]
    fn test_is_in_range() {
        assert!(IPRangeManager::is_in_range("192.168.1.50", "192.168.1.0", "192.168.1.255"));
        assert!(!IPRangeManager::is_in_range("10.0.0.1", "192.168.1.0", "192.168.1.255"));
    }
}
