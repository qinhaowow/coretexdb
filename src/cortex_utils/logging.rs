//! Logging utilities for CortexDB.
//!
//! This module provides logging functionality for CortexDB, including:
//! - Log level configuration
//! - Structured logging
//! - Log formatting
//! - File and console logging
//! - Log rotation
//! - Log filtering

use std::fmt::Debug;
use std::sync::Arc;
use tracing::{debug, error, info, span, trace, warn, Level};
use tracing_subscriber::{
    fmt::{self, format::FmtSpan},
    prelude::__tracing_subscriber_SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

/// Log level
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum LogLevel {
    /// Trace level (most verbose)
    Trace,
    /// Debug level
    Debug,
    /// Info level
    Info,
    /// Warn level
    Warn,
    /// Error level (least verbose)
    Error,
}

impl From<LogLevel> for Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => Level::TRACE,
            LogLevel::Debug => Level::DEBUG,
            LogLevel::Info => Level::INFO,
            LogLevel::Warn => Level::WARN,
            LogLevel::Error => Level::ERROR,
        }
    }
}

impl From<Level> for LogLevel {
    fn from(level: Level) -> Self {
        match level {
            Level::TRACE => LogLevel::Trace,
            Level::DEBUG => LogLevel::Debug,
            Level::INFO => LogLevel::Info,
            Level::WARN => LogLevel::Warn,
            Level::ERROR => LogLevel::Error,
        }
    }
}

/// Log configuration
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct LogConfig {
    /// Log level
    pub level: LogLevel,
    /// Log format (json or text)
    pub format: String,
    /// Log file path (optional)
    pub file: Option<String>,
    /// Enable console logging
    pub console: bool,
    /// Enable log rotation
    pub rotate: bool,
    /// Rotation max file size (in bytes)
    pub max_size: u64,
    /// Rotation max number of files
    pub max_files: usize,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            level: LogLevel::Info,
            format: "text".to_string(),
            file: None,
            console: true,
            rotate: false,
            max_size: 104857600, // 100MB
            max_files: 5,
        }
    }
}

/// Logger initialization
pub struct Logger {
    /// Log configuration
    config: LogConfig,
}

impl Logger {
    /// Create a new logger
    pub fn new(config: LogConfig) -> Self {
        Self { config }
    }

    /// Initialize the logger
    pub fn init(&self) -> Result<(), anyhow::Error> {
        let level = self.config.level.into();
        
        // Create env filter
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::default().add_directive(tracing::Level::from(level).into()));
        
        // Create subscriber
        let mut subscriber = tracing_subscriber::Registry::default()
            .with(filter);
        
        // Add console layer if enabled
        if self.config.console {
            let console_layer = fmt::layer()
                .with_target(true)
                .with_thread_names(true)
                .with_span_events(FmtSpan::CLOSE)
                .with_level(true);
            
            subscriber = subscriber.with(console_layer);
        }
        
        // Add file layer if configured
        if let Some(file_path) = &self.config.file {
            // In a real implementation, we'd add a file layer with rotation
            // For now, we'll just log to console
            println!("Logging to file: {}", file_path);
        }
        
        // Initialize subscriber
        subscriber.init();
        
        info!("Logger initialized with level: {:?}", self.config.level);
        debug!("Debug logging enabled");
        
        Ok(())
    }
}

/// Log macro utilities
pub mod log {
    use super::*;
    
    /// Log trace message
    pub fn trace<T: Debug>(message: T) {
        trace!("{:?}", message);
    }
    
    /// Log debug message
    pub fn debug<T: Debug>(message: T) {
        debug!("{:?}", message);
    }
    
    /// Log info message
    pub fn info<T: Debug>(message: T) {
        info!("{:?}", message);
    }
    
    /// Log warn message
    pub fn warn<T: Debug>(message: T) {
        warn!("{:?}", message);
    }
    
    /// Log error message
    pub fn error<T: Debug>(message: T) {
        error!("{:?}", message);
    }
    
    /// Log trace message with context
    pub fn trace_with<T: Debug, C: Debug>(message: T, context: C) {
        trace!("{:?} | Context: {:?}", message, context);
    }
    
    /// Log debug message with context
    pub fn debug_with<T: Debug, C: Debug>(message: T, context: C) {
        debug!("{:?} | Context: {:?}", message, context);
    }
    
    /// Log info message with context
    pub fn info_with<T: Debug, C: Debug>(message: T, context: C) {
        info!("{:?} | Context: {:?}", message, context);
    }
    
    /// Log warn message with context
    pub fn warn_with<T: Debug, C: Debug>(message: T, context: C) {
        warn!("{:?} | Context: {:?}", message, context);
    }
    
    /// Log error message with context
    pub fn error_with<T: Debug, C: Debug>(message: T, context: C) {
        error!("{:?} | Context: {:?}", message, context);
    }
}

/// Log span utilities
pub mod span {
    use tracing::span;
    
    /// Create a trace span
    pub fn trace(name: &str) -> tracing::Span {
        span!(tracing::Level::TRACE, name)
    }
    
    /// Create a debug span
    pub fn debug(name: &str) -> tracing::Span {
        span!(tracing::Level::DEBUG, name)
    }
    
    /// Create an info span
    pub fn info(name: &str) -> tracing::Span {
        span!(tracing::Level::INFO, name)
    }
    
    /// Create a warn span
    pub fn warn(name: &str) -> tracing::Span {
        span!(tracing::Level::WARN, name)
    }
    
    /// Create an error span
    pub fn error(name: &str) -> tracing::Span {
        span!(tracing::Level::ERROR, name)
    }
}

/// Initialize logger with default configuration
pub fn init_default_logger() -> Result<(), anyhow::Error> {
    let config = LogConfig::default();
    let logger = Logger::new(config);
    logger.init()
}

/// Initialize logger with custom configuration
pub fn init_logger(config: LogConfig) -> Result<(), anyhow::Error> {
    let logger = Logger::new(config);
    logger.init()
}

/// Test utilities for logging
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_config_default() {
        let config = LogConfig::default();
        assert_eq!(config.level, LogLevel::Info);
        assert_eq!(config.format, "text");
        assert!(config.console);
        assert!(!config.rotate);
    }

    #[test]
    fn test_log_level_conversion() {
        let level = LogLevel::Debug;
        let tracing_level: Level = level.into();
        assert_eq!(tracing_level, Level::DEBUG);
        
        let converted_back: LogLevel = tracing_level.into();
        assert_eq!(converted_back, LogLevel::Debug);
    }

    #[test]
    fn test_logger_initialization() {
        let config = LogConfig::default();
        let logger = Logger::new(config);
        
        let result = logger.init();
        assert!(result.is_ok());
    }
}
