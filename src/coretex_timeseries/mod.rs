//! Time Series Index module for CoreTexDB
//! Supports efficient storage and querying of time-series data

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
pub struct TimeSeriesPoint {
    pub timestamp: i64,
    pub value: f64,
    pub metadata: Option<serde_json::Value>,
}

impl TimeSeriesPoint {
    pub fn new(timestamp: i64, value: f64) -> Self {
        Self {
            timestamp,
            value,
            metadata: None,
        }
    }

    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
}

pub struct TimeSeries {
    name: String,
    points: Vec<TimeSeriesPoint>,
    start_time: Option<i64>,
    end_time: Option<i64>,
}

impl TimeSeries {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            points: Vec::new(),
            start_time: None,
            end_time: None,
        }
    }

    pub fn insert(&mut self, point: TimeSeriesPoint) {
        if self.start_time.is_none() || point.timestamp < self.start_time.unwrap() {
            self.start_time = Some(point.timestamp);
        }
        if self.end_time.is_none() || point.timestamp > self.end_time.unwrap() {
            self.end_time = Some(point.timestamp);
        }
        self.points.push(point);
    }

    pub fn get_range(&self, start: i64, end: i64) -> Vec<&TimeSeriesPoint> {
        self.points
            .iter()
            .filter(|p| p.timestamp >= start && p.timestamp <= end)
            .collect()
    }

    pub fn mean(&self) -> Option<f64> {
        if self.points.is_empty() {
            return None;
        }
        let sum: f64 = self.points.iter().map(|p| p.value).sum();
        Some(sum / self.points.len() as f64)
    }

    pub fn min(&self) -> Option<f64> {
        self.points.iter().map(|p| p.value).fold(None, |acc, v| {
            Some(acc.map_or(v, |acc| acc.min(v)))
        })
    }

    pub fn max(&self) -> Option<f64> {
        self.points.iter().map(|p| p.value).fold(None, |acc, v| {
            Some(acc.map_or(v, |acc| acc.max(v)))
        })
    }

    pub fn count(&self) -> usize {
        self.points.len()
    }

    pub fn downsample(&self, bucket_size: i64, agg: Aggregation) -> Vec<TimeSeriesPoint> {
        if self.points.is_empty() || bucket_size <= 0 {
            return vec![];
        }

        let mut buckets: HashMap<i64, Vec<f64>> = HashMap::new();
        
        for point in &self.points {
            let bucket_key = (point.timestamp / bucket_size) * bucket_size;
            buckets.entry(bucket_key).or_default().push(point.value);
        }

        let mut results: Vec<TimeSeriesPoint> = buckets
            .into_iter()
            .map(|(timestamp, values)| {
                let aggregated = match agg {
                    Aggregation::Mean => values.iter().sum::<f64>() / values.len() as f64,
                    Aggregation::Sum => values.iter().sum(),
                    Aggregation::Min => values.iter().cloned().fold(f64::INFINITY, f64::min),
                    Aggregation::Max => values.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
                    Aggregation::Count => values.len() as f64,
                    Aggregation::First => *values.first().unwrap_or(&0.0),
                    Aggregation::Last => *values.last().unwrap_or(&0.0),
                    Aggregation::Std => {
                        let mean = values.iter().sum::<f64>() / values.len() as f64;
                        let variance = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                        variance.sqrt()
                    }
                };
                TimeSeriesPoint::new(timestamp, aggregated)
            })
            .collect();

        results.sort_by_key(|p| p.timestamp);
        results
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Aggregation {
    Mean,
    Sum,
    Min,
    Max,
    Count,
    First,
    Last,
    Std,
}

pub struct TimeSeriesIndex {
    series: Arc<RwLock<HashMap<String, TimeSeries>>>,
    retention_period: Duration,
}

impl TimeSeriesIndex {
    pub fn new() -> Self {
        Self {
            series: Arc::new(RwLock::new(HashMap::new())),
            retention_period: Duration::from_secs(365 * 24 * 60 * 60),
        }
    }

    pub fn with_retention(mut self, days: u64) -> Self {
        self.retention_period = Duration::from_secs(days * 24 * 60 * 60);
        self
    }

    pub async fn create_series(&self, name: &str) {
        let mut series_map = self.series.write().await;
        series_map.insert(name.to_string(), TimeSeries::new(name));
    }

    pub async fn insert(&self, series_name: &str, point: TimeSeriesPoint) -> Result<(), String> {
        let mut series_map = self.series.write().await;
        if let Some(series) = series_map.get_mut(series_name) {
            series.insert(point);
            Ok(())
        } else {
            Err(format!("Series '{}' not found", series_name))
        }
    }

    pub async fn insert_batch(&self, series_name: &str, points: Vec<TimeSeriesPoint>) -> Result<usize, String> {
        let mut series_map = self.series.write().await;
        if let Some(series) = series_map.get_mut(series_name) {
            for point in points {
                series.insert(point);
            }
            Ok(series.count())
        } else {
            Err(format!("Series '{}' not found", series_name))
        }
    }

    pub async fn query_range(&self, series_name: &str, start: i64, end: i64) -> Result<Vec<TimeSeriesPoint>, String> {
        let series_map = self.series.read().await;
        if let Some(series) = series_map.get(series_name) {
            Ok(series.get_range(start, end).into_iter().cloned().collect())
        } else {
            Err(format!("Series '{}' not found", series_name))
        }
    }

    pub async fn query_aggregated(
        &self,
        series_name: &str,
        start: i64,
        end: i64,
        bucket_size: i64,
        agg: Aggregation,
    ) -> Result<Vec<TimeSeriesPoint>, String> {
        let series_map = self.series.read().await;
        if let Some(series) = series_map.get(series_name) {
            let filtered: Vec<TimeSeriesPoint> = series
                .get_range(start, end)
                .into_iter()
                .cloned()
                .collect();
            
            let temp_series = TimeSeries::new(series_name);
            let mut temp = temp_series;
            for p in filtered {
                temp.insert(p);
            }
            
            Ok(temp.downsample(bucket_size, agg))
        } else {
            Err(format!("Series '{}' not found", series_name))
        }
    }

    pub async fn get_stats(&self, series_name: &str) -> Result<TimeSeriesStats, String> {
        let series_map = self.series.read().await;
        if let Some(series) = series_map.get(series_name) {
            Ok(TimeSeriesStats {
                count: series.count(),
                mean: series.mean(),
                min: series.min(),
                max: series.max(),
                start_time: series.start_time,
                end_time: series.end_time,
            })
        } else {
            Err(format!("Series '{}' not found", series_name))
        }
    }

    pub async fn list_series(&self) -> Vec<String> {
        let series_map = self.series.read().await;
        series_map.keys().cloned().collect()
    }

    pub async fn delete_series(&self, name: &str) -> bool {
        let mut series_map = self.series.write().await;
        series_map.remove(name).is_some()
    }

    pub async fn delete_old(&self, before_timestamp: i64) -> usize {
        let mut series_map = self.series.write().await;
        let mut deleted_count = 0;

        for series in series_map.values_mut() {
            let original_len = series.count();
            series.points.retain(|p| p.timestamp >= before_timestamp);
            deleted_count += original_len - series.count();
        }

        deleted_count
    }
}

impl Default for TimeSeriesIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct TimeSeriesStats {
    pub count: usize,
    pub mean: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
}

pub struct RollingWindow {
    window_size: usize,
    data: VecDeque<f64>,
    sum: f64,
}

impl RollingWindow {
    pub fn new(window_size: usize) -> Self {
        Self {
            window_size,
            data: VecDeque::new(),
            sum: 0.0,
        }
    }

    pub fn push(&mut self, value: f64) {
        if self.data.len() >= self.window_size {
            if let Some(removed) = self.data.pop_front() {
                self.sum -= removed;
            }
        }
        self.data.push_back(value);
        self.sum += value;
    }

    pub fn mean(&self) -> Option<f64> {
        if self.data.is_empty() {
            return None;
        }
        Some(self.sum / self.data.len() as f64)
    }

    pub fn std(&self) -> Option<f64> {
        if self.data.is_empty() {
            return None;
        }
        let mean = self.sum / self.data.len() as f64;
        let variance = self.data.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / self.data.len() as f64;
        Some(variance.sqrt())
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_full(&self) -> bool {
        self.data.len() >= self.window_size
    }
}

pub struct ExponentialMovingAverage {
    alpha: f64,
    previous_ema: Option<f64>,
}

impl ExponentialMovingAverage {
    pub fn new(alpha: f64) -> Self {
        Self {
            alpha: alpha.clamp(0.0, 1.0),
            previous_ema: None,
        }
    }

    pub fn update(&mut self, value: f64) -> f64 {
        match self.previous_ema {
            Some(ema) => {
                let new_ema = self.alpha * value + (1.0 - self.alpha) * ema;
                self.previous_ema = Some(new_ema);
                new_ema
            }
            None => {
                self.previous_ema = Some(value);
                value
            }
        }
    }

    pub fn reset(&mut self) {
        self.previous_ema = None;
    }
}

pub struct TimeSeriesIterator {
    series: TimeSeries,
    current_idx: usize,
}

impl Iterator for TimeSeriesIterator {
    type Item = TimeSeriesPoint;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_idx < self.series.points.len() {
            let point = self.series.points[self.current_idx].clone();
            self.current_idx += 1;
            Some(point)
        } else {
            None
        }
    }
}

impl IntoIterator for TimeSeries {
    type Item = TimeSeriesPoint;
    type IntoIter = TimeSeriesIterator;

    fn into_iter(self) -> Self::IntoIter {
        TimeSeriesIterator {
            series: self,
            current_idx: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timeseries_insert() {
        let mut series = TimeSeries::new("test");
        
        series.insert(TimeSeriesPoint::new(1000, 10.0));
        series.insert(TimeSeriesPoint::new(2000, 20.0));
        
        assert_eq!(series.count(), 2);
    }

    #[test]
    fn test_timeseries_stats() {
        let mut series = TimeSeries::new("test");
        
        series.insert(TimeSeriesPoint::new(1000, 10.0));
        series.insert(TimeSeriesPoint::new(2000, 20.0));
        series.insert(TimeSeriesPoint::new(3000, 30.0));
        
        assert_eq!(series.mean(), Some(20.0));
        assert_eq!(series.min(), Some(10.0));
        assert_eq!(series.max(), Some(30.0));
    }

    #[test]
    fn test_downsample() {
        let mut series = TimeSeries::new("test");
        
        for i in 0..10 {
            series.insert(TimeSeriesPoint::new(i * 1000, i as f64));
        }
        
        let downsampled = series.downsample(3000, Aggregation::Mean);
        
        assert!(!downsampled.is_empty());
    }

    #[tokio::test]
    async fn test_timeseries_index() {
        let index = TimeSeriesIndex::new();
        
        index.create_series("cpu").await;
        
        index.insert("cpu", TimeSeriesPoint::new(1000, 50.0)).await.unwrap();
        index.insert("cpu", TimeSeriesPoint::new(2000, 60.0)).await.unwrap();
        
        let stats = index.get_stats("cpu").await.unwrap();
        assert_eq!(stats.count, 2);
    }

    #[test]
    fn test_rolling_window() {
        let mut window = RollingWindow::new(3);
        
        window.push(1.0);
        window.push(2.0);
        window.push(3.0);
        
        assert_eq!(window.mean(), Some(2.0));
        assert!(window.is_full());
        
        window.push(4.0);
        assert_eq!(window.mean(), Some(3.0));
    }

    #[test]
    fn test_ema() {
        let mut ema = ExponentialMovingAverage::new(0.5);
        
        assert_eq!(ema.update(10.0), 10.0);
        assert_eq!(ema.update(20.0), 15.0);
        assert_eq!(ema.update(40.0), 27.5);
    }
}
