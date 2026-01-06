//! Utility functions for CortexDB CLI.
//!
//! This module provides utility functions for the CortexDB CLI, including:
//! - Input/output utilities
//! - File handling utilities
//! - JSON processing utilities
//! - Error formatting utilities
//! - Progress bar utilities

use colored::{Color, Colorize};
use serde_json;
use std::fs;
use std::io::{self, Write};
use std::path::Path;
use std::time::Duration;

/// Print success message
pub fn print_success(message: &str) {
    println!("{} {}", "✓".green(), message);
}

/// Print error message
pub fn print_error(message: &str) {
    println!("{} {}", "✗".red(), message);
}

/// Print warning message
pub fn print_warning(message: &str) {
    println!("{} {}", "⚠️".yellow(), message);
}

/// Print info message
pub fn print_info(message: &str) {
    println!("{} {}", "ℹ️".blue(), message);
}

/// Print debug message
pub fn print_debug(message: &str) {
    if std::env::var("RUST_LOG").unwrap_or_default().contains("debug") {
        println!("{} {}", "🐛".purple(), message);
    }
}

/// Read JSON from file
pub fn read_json_file<P: AsRef<Path>>(path: P) -> Result<serde_json::Value, anyhow::Error> {
    let content = fs::read_to_string(path)?;
    let value = serde_json::from_str(&content)?;
    Ok(value)
}

/// Write JSON to file
pub fn write_json_file<P: AsRef<Path>>(path: P, value: &serde_json::Value) -> Result<(), anyhow::Error> {
    let content = serde_json::to_string_pretty(value)?;
    fs::write(path, content)?;
    Ok(())
}

/// Read line from stdin with prompt
pub fn read_line(prompt: &str) -> Result<String, io::Error> {
    print!("{} ", prompt);
    io::stdout().flush()?;
    
    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    
    Ok(line.trim().to_string())
}

/// Ask yes/no question
pub fn ask_yes_no(question: &str) -> Result<bool, io::Error> {
    loop {
        let response = read_line(&format!("{} (y/n):", question))?;
        match response.to_lowercase().as_str() {
            "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => print_warning("Please enter y or n"),
        }
    }
}

/// Format duration for display
pub fn format_duration(duration: Duration) -> String {
    let millis = duration.as_millis();
    
    if millis < 1000 {
        format!("{}ms", millis)
    } else {
        let seconds = millis as f64 / 1000.0;
        format!("{:.2}s", seconds)
    }
}

/// Format bytes for display
pub fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_index = 0;
    
    while size >= 1024.0 && unit_index < UNITS.len() - 1 {
        size /= 1024.0;
        unit_index += 1;
    }
    
    format!("{:.2} {}", size, UNITS[unit_index])
}

/// Truncate string with ellipsis
pub fn truncate_string(s: &str, max_length: usize) -> String {
    if s.len() <= max_length {
        s.to_string()
    } else {
        format!("{}...", &s[..max_length - 3])
    }
}

/// Check if file exists
pub fn file_exists<P: AsRef<Path>>(path: P) -> bool {
    path.as_ref().exists()
}

/// Check if directory exists
pub fn directory_exists<P: AsRef<Path>>(path: P) -> bool {
    path.as_ref().is_dir()
}

/// Create directory if it doesn't exist
pub fn create_directory<P: AsRef<Path>>(path: P) -> Result<(), anyhow::Error> {
    if !directory_exists(path.as_ref()) {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

/// Get file extension
pub fn get_file_extension<P: AsRef<Path>>(path: P) -> Option<String> {
    path.as_ref().extension().and_then(|ext| ext.to_str().map(|s| s.to_lowercase()))
}

/// Check if file is JSON
pub fn is_json_file<P: AsRef<Path>>(path: P) -> bool {
    get_file_extension(path).map(|ext| ext == "json").unwrap_or(false)
}

/// Check if file is YAML
pub fn is_yaml_file<P: AsRef<Path>>(path: P) -> bool {
    let ext = get_file_extension(path);
    ext.map(|ext| ext == "yaml" || ext == "yml").unwrap_or(false)
}

/// Parse JSON string
pub fn parse_json(json_str: &str) -> Result<serde_json::Value, serde_json::Error> {
    serde_json::from_str(json_str)
}

/// Serialize value to JSON string
pub fn to_json<T: serde::Serialize>(value: &T) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(value)
}

/// Format error for display
pub fn format_error(error: &anyhow::Error) -> String {
    error.to_string()
}

/// Format CortexError for display
pub fn format_cortex_error(error: &crate::cortex_core::error::CortexError) -> String {
    error.to_string()
}

/// Create progress bar (placeholder)
pub fn create_progress_bar(length: u64) -> ProgressBar {
    ProgressBar {
        length,
        current: 0,
    }
}

/// Progress bar struct
pub struct ProgressBar {
    length: u64,
    current: u64,
}

impl ProgressBar {
    /// Increment progress
    pub fn increment(&mut self, amount: u64) {
        self.current = std::cmp::min(self.current + amount, self.length);
        self.update();
    }

    /// Update progress display
    pub fn update(&self) {
        let percentage = (self.current as f64 / self.length as f64) * 100.0;
        print!("\r[{}{}] {:.1}%", 
            "=".repeat((percentage / 5.0) as usize),
            " ".repeat(20 - (percentage / 5.0) as usize),
            percentage
        );
        std::io::stdout().flush().unwrap();
    }

    /// Finish progress
    pub fn finish(&self) {
        println!();
    }
}

/// Sleep for specified duration
pub fn sleep(duration: Duration) {
    std::thread::sleep(duration);
}

/// Get current timestamp
pub fn get_timestamp() -> String {
    let now = chrono::Utc::now();
    now.format("%Y-%m-%d %H:%M:%S").to_string()
}

/// Get current date
pub fn get_date() -> String {
    let now = chrono::Utc::now();
    now.format("%Y-%m-%d").to_string()
}

/// Get current time
pub fn get_time() -> String {
    let now = chrono::Utc::now();
    now.format("%H:%M:%S").to_string()
}
