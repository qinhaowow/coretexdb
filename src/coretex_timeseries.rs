//! Time-Series Data Module for CortexDB
//! Specialized for stock/financial data scenarios

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use tokio::sync::RwLock;

pub struct TimeSeriesIndex {
    data: Arc<RwLock<HashMap<String, TimeSeries>>>,
    timestamps: Arc<RwLock<HashMap<String, Vec<TimestampEntry>>>>,
    config: TimeSeriesConfig,
}

#[derive(Debug, Clone)]
pub struct TimeSeriesConfig {
    pub retention_days: u32,
    pub chunk_size: usize,
    pub enable_compression: bool,
    pub time_field: String,
}

impl Default for TimeSeriesConfig {
    fn default() -> Self {
        Self {
            retention_days: 365,
            chunk_size: 1000,
            enable_compression: false,
            time_field: "timestamp".to_string(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TimeSeries {
    pub id: String,
    pub dimension: usize,
    pub data_points: Vec<DataPoint>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct DataPoint {
    pub timestamp: i64,
    pub vector: Vec<f32>,
    pub value: Option<f64>,
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct TimestampEntry {
    pub timestamp: i64,
    pub offset: usize,
    pub data_id: String,
}

impl TimeSeriesIndex {
    pub fn new(config: TimeSeriesConfig) -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            timestamps: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    pub async fn create_series(&self, id: &str, dimension: usize) -> Result<(), TimeSeriesError> {
        let mut data = self.data.write().await;
        
        if data.contains_key(id) {
            return Err(TimeSeriesError::SeriesExists(id.to_string()));
        }

        data.insert(id.to_string(), TimeSeries {
            id: id.to_string(),
            dimension,
            data_points: Vec::new(),
            metadata: HashMap::new(),
        });

        let mut ts = self.timestamps.write().await;
        ts.insert(id.to_string(), Vec::new());

        Ok(())
    }

    pub async fn append(&self, series_id: &str, data_point: DataPoint) -> Result<(), TimeSeriesError> {
        let mut data = self.data.write().await;
        
        let series = data.get_mut(series_id)
            .ok_or(TimeSeriesError::SeriesNotFound(series_id.to_string()))?;

        if data_point.vector.len() != series.dimension {
            return Err(TimeSeriesError::InvalidDimension(format!(
                "Expected {}, got {}",
                series.dimension,
                data_point.vector.len()
            )));
        }

        let offset = series.data_points.len();
        series.data_points.push(data_point.clone());

        drop(data);

        let mut timestamps = self.timestamps.write().await;
        if let Some(entries) = timestamps.get_mut(series_id) {
            entries.push(TimestampEntry {
                timestamp: data_point.timestamp,
                offset,
                data_id: series_id.to_string(),
            });
            entries.sort_by_key(|e| e.timestamp);
        }

        Ok(())
    }

    pub async fn append_batch(&self, series_id: &str, data_points: Vec<DataPoint>) -> Result<(), TimeSeriesError> {
        for point in data_points {
            self.append(series_id, point).await?;
        }
        Ok(())
    }

    pub async fn get_range(
        &self,
        series_id: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<Vec<DataPoint>, TimeSeriesError> {
        let data = self.data.read().await;
        
        let series = data.get(series_id)
            .ok_or(TimeSeriesError::SeriesNotFound(series_id.to_string()))?;

        let results: Vec<DataPoint> = series.data_points
            .iter()
            .filter(|dp| dp.timestamp >= start_time && dp.timestamp <= end_time)
            .cloned()
            .collect();

        Ok(results)
    }

    pub async fn get_latest(&self, series_id: &str, count: usize) -> Result<Vec<DataPoint>, TimeSeriesError> {
        let data = self.data.read().await;
        
        let series = data.get(series_id)
            .ok_or(TimeSeriesError::SeriesNotFound(series_id.to_string()))?;

        let start = series.data_points.len().saturating_sub(count);
        let results: Vec<DataPoint> = series.data_points[start..].to_vec();

        Ok(results)
    }

    pub async fn window_aggregate(
        &self,
        series_id: &str,
        window_size: i64,
        agg_type: AggregationType,
    ) -> Result<Vec<AggregatedPoint>, TimeSeriesError> {
        let data = self.data.read().await;
        
        let series = data.get(series_id)
            .ok_or(TimeSeriesError::SeriesNotFound(series_id.to_string()))?;

        if series.data_points.is_empty() {
            return Ok(Vec::new());
        }

        let min_ts = series.data_points.first().map(|dp| dp.timestamp).unwrap();
        let max_ts = series.data_points.last().map(|dp| dp.timestamp).unwrap();

        let mut windows: HashMap<i64, Vec<&DataPoint>> = HashMap::new();
        
        for dp in &series.data_points {
            let window_key = (dp.timestamp - min_ts) / window_size;
            let window_ts = min_ts + window_key * window_size;
            windows.entry(window_ts).or_insert_with(Vec::new).push(dp);
        }

        let mut results: Vec<AggregatedPoint> = windows
            .into_iter()
            .map(|(window_start, points)| {
                let vector = Self::aggregate_vectors(&points, &agg_type);
                let value = Self::aggregate_values(points, &agg_type);
                
                AggregatedPoint {
                    window_start,
                    window_end: window_start + window_size,
                    vector,
                    value,
                    count: points.len(),
                }
            })
            .collect();

        results.sort_by_key(|r| r.window_start);

        Ok(results)
    }

    fn aggregate_vectors(points: &[&DataPoint], agg_type: &AggregationType) -> Vec<f32> {
        if points.is_empty() {
            return Vec::new();
        }

        let dim = points[0].vector.len();
        match agg_type {
            AggregationType::Mean => {
                let mut sum = vec![0.0f32; dim];
                for p in points {
                    for (i, v) in p.vector.iter().enumerate() {
                        sum[i] += v;
                    }
                }
                for v in sum.iter_mut() {
                    *v /= points.len() as f32;
                }
                sum
            },
            AggregationType::Max => {
                let mut max = vec![f32::MIN; dim];
                for p in points {
                    for (i, v) in p.vector.iter().enumerate() {
                        max[i] = max[i].max(*v);
                    }
                }
                max
            },
            AggregationType::Min => {
                let mut min = vec![f32::MAX; dim];
                for p in points {
                    for (i, v) in p.vector.iter().enumerate() {
                        min[i] = min[i].min(*v);
                    }
                }
                min
            },
            AggregationType::Sum => {
                let mut sum = vec![0.0f32; dim];
                for p in points {
                    for (i, v) in p.vector.iter().enumerate() {
                        sum[i] += v;
                    }
                }
                sum
            },
            AggregationType::Last => {
                points.last().map(|p| p.vector.clone()).unwrap_or_default()
            },
            AggregationType::First => {
                points.first().map(|p| p.vector.clone()).unwrap_or_default()
            },
        }
    }

    fn aggregate_values(points: &[&DataPoint], agg_type: &AggregationType) -> Option<f64> {
        let values: Vec<f64> = points
            .iter()
            .filter_map(|p| p.value)
            .collect();

        if values.is_empty() {
            return None;
        }

        match agg_type {
            AggregationType::Mean => Some(values.iter().sum::<f64>() / values.len() as f64),
            AggregationType::Max => values.iter().cloned().reduce(f64::max),
            AggregationType::Min => values.iter().cloned().reduce(f64::min),
            AggregationType::Sum => Some(values.iter().sum()),
            AggregationType::Last => values.last().copied(),
            AggregationType::First => values.first().copied(),
        }
    }

    pub async fn time_weighted_average(
        &self,
        series_id: &str,
        start_time: i64,
        end_time: i64,
    ) -> Result<f64, TimeSeriesError> {
        let data = self.data.read().await;
        
        let series = data.get(series_id)
            .ok_or(TimeSeriesError::SeriesNotFound(series_id.to_string()))?;

        let points: Vec<&DataPoint> = series.data_points
            .iter()
            .filter(|dp| dp.timestamp >= start_time && dp.timestamp <= end_time)
            .filter(|dp| dp.value.is_some())
            .collect();

        if points.len() < 2 {
            return points.first()
                .and_then(|p| p.value)
                .ok_or(TimeSeriesError::NoData);
        }

        let mut twa = 0.0;
        let mut total_weight = 0.0;

        for i in 0..points.len() - 1 {
            let value = points[i].value.unwrap();
            let dt = (points[i + 1].timestamp - points[i].timestamp) as f64;
            twa += value * dt;
            total_weight += dt;
        }

        if total_weight > 0.0 {
            Ok(twa / total_weight)
        } else {
            Err(TimeSeriesError::NoData)
        }
    }

    pub async fn delete_old_data(&self, series_id: &str, before_timestamp: i64) -> Result<usize, TimeSeriesError> {
        let mut data = self.data.write().await;
        
        let series = data.get_mut(series_id)
            .ok_or(TimeSeriesError::SeriesNotFound(series_id.to_string()))?;

        let original_len = series.data_points.len();
        series.data_points.retain(|dp| dp.timestamp >= before_timestamp);
        let deleted = original_len - series.data_points.len();

        drop(data);

        let mut timestamps = self.timestamps.write().await;
        if let Some(entries) = timestamps.get_mut(series_id) {
            entries.retain(|e| e.timestamp >= before_timestamp);
        }

        Ok(deleted)
    }

    pub async fn get_stats(&self, series_id: &str) -> Result<TimeSeriesStats, TimeSeriesError> {
        let data = self.data.read().await;
        
        let series = data.get(series_id)
            .ok_or(TimeSeriesError::SeriesNotFound(series_id.to_string()))?;

        let timestamps: Vec<i64> = series.data_points.iter().map(|dp| dp.timestamp).collect();
        let min_ts = timestamps.iter().min().copied();
        let max_ts = timestamps.iter().max().copied();

        let values: Vec<f64> = series.data_points.iter().filter_map(|dp| dp.value).collect();
        let avg_value = if values.is_empty() {
            None
        } else {
            Some(values.iter().sum::<f64>() / values.len() as f64)
        };

        Ok(TimeSeriesStats {
            series_id: series_id.to_string(),
            data_points: series.data_points.len(),
            start_time: min_ts,
            end_time: max_ts,
            average_value: avg_value,
        })
    }
}

#[derive(Debug, Clone)]
pub enum AggregationType {
    Mean,
    Max,
    Min,
    Sum,
    First,
    Last,
}

#[derive(Debug, Clone)]
pub struct AggregatedPoint {
    pub window_start: i64,
    pub window_end: i64,
    pub vector: Vec<f32>,
    pub value: Option<f64>,
    pub count: usize,
}

#[derive(Debug, Clone)]
pub struct TimeSeriesStats {
    pub series_id: String,
    pub data_points: usize,
    pub start_time: Option<i64>,
    pub end_time: Option<i64>,
    pub average_value: Option<f64>,
}

#[derive(Debug)]
pub enum TimeSeriesError {
    SeriesNotFound(String),
    SeriesExists(String),
    InvalidDimension(String),
    NoData,
}

impl std::fmt::Display for TimeSeriesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeSeriesError::SeriesNotFound(id) => write!(f, "Series not found: {}", id),
            TimeSeriesError::SeriesExists(id) => write!(f, "Series already exists: {}", id),
            TimeSeriesError::InvalidDimension(msg) => write!(f, "Invalid dimension: {}", msg),
            TimeSeriesError::NoData => write!(f, "No data available"),
        }
    }
}

impl std::error::Error for TimeSeriesError {}

pub struct TimeSeriesSimilarity;

impl TimeSeriesSimilarity {
    pub fn euclidean_distance(a: &[f32], b: &[f32]) -> f32 {
        a.iter()
            .zip(b.iter())
            .map(|(x, y)| (x - y).powi(2))
            .sum::<f32>()
            .sqrt()
    }

    pub fn dynamic_time_warping(series1: &[f32], series2: &[f32]) -> f32 {
        let n = series1.len();
        let m = series2.len();
        
        if n == 0 || m == 0 {
            return f32::MAX;
        }

        let mut dtw = vec![vec![f32::MAX; m + 1]; n + 1];
        dtw[0][0] = 0.0;

        for i in 1..=n {
            for j in 1..=m {
                let cost = (series1[i - 1] - series2[j - 1]).powi(2);
                dtw[i][j] = cost + dtw[i - 1][j - 1]
                    .min(dtw[i - 1][j])
                    .min(dtw[i][j - 1]);
            }
        }

        dtw[n][m].sqrt()
    }

    pub fn cross_correlation(series1: &[f32], series2: &[f32]) -> f32 {
        let n = series1.len().min(series2.len());
        if n == 0 {
            return 0.0;
        }

        let mean1: f32 = series1.iter().take(n).sum::<f32>() / n as f32;
        let mean2: f32 = series2.iter().take(n).sum::<f32>() / n as f32;

        let mut cov = 0.0;
        let mut var1 = 0.0;
        let mut var2 = 0.0;

        for i in 0..n {
            let d1 = series1[i] - mean1;
            let d2 = series2[i] - mean2;
            cov += d1 * d2;
            var1 += d1 * d1;
            var2 += d2 * d2;
        }

        let denom = (var1 * var2).sqrt();
        if denom > 0.0 {
            cov / denom
        } else {
            0.0
        }
    }

    pub fn find_pattern(
        series: &[f32],
        pattern: &[f32],
        threshold: f32,
    ) -> Vec<PatternMatch> {
        let mut matches = Vec::new();
        
        if pattern.len() > series.len() {
            return matches;
        }

        for i in 0..=series.len() - pattern.len() {
            let window = &series[i..i + pattern.len()];
            let distance = Self::euclidean_distance(window, pattern);
            
            if distance < threshold {
                matches.push(PatternMatch {
                    start_index: i,
                    end_index: i + pattern.len(),
                    distance,
                });
            }
        }

        matches.sort_by(|a, b| a.distance.partial_cmp(&b.distance).unwrap());
        matches
    }

    pub fn shape_similarity(series1: &[f32], series2: &[f32]) -> f32 {
        if series1.len() != series2.len() || series1.is_empty() {
            return 1.0;
        }

        let normalize = |s: &[f32]| -> Vec<f32> {
            let mean = s.iter().sum::<f32>() / s.len() as f32;
            let std = (s.iter().map(|x| (x - mean).powi(2)).sum::<f32>() / s.len() as f32).sqrt();
            if std > 0.0 {
                s.iter().map(|x| (x - mean) / std).collect()
            } else {
                s.iter().map(|_| 0.0).collect()
            }
        };

        let norm1 = normalize(series1);
        let norm2 = normalize(series2);

        1.0 - Self::euclidean_distance(&norm1, &norm2).min(1.0)
    }
}

#[derive(Debug, Clone)]
pub struct PatternMatch {
    pub start_index: usize,
    pub end_index: usize,
    pub distance: f32,
}
