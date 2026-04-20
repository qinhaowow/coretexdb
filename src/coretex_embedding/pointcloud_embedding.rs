//! Point cloud and 3D data embedding service

use std::error::Error;

#[derive(Debug, Clone)]
pub struct PointCloudEmbeddingService {
    model_name: String,
    dimension: usize,
    device: String,
    max_points: usize,
}

impl PointCloudEmbeddingService {
    pub fn new(model_name: &str, dimension: usize, device: &str, max_points: usize) -> Self {
        Self {
            model_name: model_name.to_string(),
            dimension,
            device: device.to_string(),
            max_points,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(
            "pointnet2",
            1024,
            "cpu",
            2048,
        )
    }

    pub fn embed_point_cloud(&self, points: &[[f32; 3]]) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        if points.is_empty() {
            return Ok(vec![0.0; self.dimension]);
        }

        let mut embedding = vec![0.0; self.dimension];

        let num_features = 7;
        let feature_dim = self.dimension / num_features;

        let mut min = [f32::MAX; 3];
        let mut max = [f32::MIN; 3];
        let mut centroid = [0.0f32; 3];
        
        for point in points {
            for i in 0..3 {
                min[i] = min[i].min(point[i]);
                max[i] = max[i].max(point[i]);
                centroid[i] += point[i];
            }
        }
        
        for i in 0..3 {
            centroid[i] /= points.len() as f32;
        }

        let extent = [
            (max[0] - min[0]).max(0.001),
            (max[1] - min[1]).max(0.001),
            (max[2] - min[2]).max(0.001),
        ];

        let mut densities = vec![0.0f32; points.len()];
        for (i, point) in points.iter().enumerate() {
            for j in points.iter() {
                let dist = ((point[0] - j[0]).powi(2) 
                         + (point[1] - j[1]).powi(2) 
                         + (point[2] - j[2]).powi(2)).sqrt();
                if dist < 0.1 {
                    densities[i] += 1.0;
                }
            }
        }

        for i in 0..feature_dim {
            let idx = i % points.len();
            let point = points[idx];
            
            embedding[i] = (point[0] - centroid[0]) / extent[0];
            embedding[i + feature_dim] = (point[1] - centroid[1]) / extent[1];
            embedding[i + feature_dim * 2] = (point[2] - centroid[2]) / extent[2];
            embedding[i + feature_dim * 3] = densities[idx] / points.len() as f32;
            embedding[i + feature_dim * 4] = extent[0] / (extent[0] + extent[1] + extent[2]);
            embedding[i + feature_dim * 5] = extent[1] / (extent[0] + extent[1] + extent[2]);
            embedding[i + feature_dim * 6] = extent[2] / (extent[0] + extent[1] + extent[2]);
        }

        self.normalize(&mut embedding);
        
        Ok(embedding)
    }

    pub fn embed_point_cloud_path(&self, path: &str) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        let data = std::fs::read_to_string(path)?;
        
        let points: Vec<[f32; 3]> = data
            .lines()
            .filter_map(|line| {
                let coords: Vec<f32> = line
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();
                if coords.len() >= 3 {
                    Some([coords[0], coords[1], coords[2]])
                } else {
                    None
                }
            })
            .collect();
        
        self.embed_point_cloud(&points)
    }

    pub fn embed_voxel_grid(&self, voxel_data: &[u8], dimensions: (u32, u32, u32)) -> Result<Vec<f32>, Box<dyn Error + Send + Sync>> {
        let total_points = (dimensions.0 * dimensions.1 * dimensions.2) as usize;
        let mut points: Vec<[f32; 3]> = Vec::with_capacity(total_points);
        
        for z in 0..dimensions.2 {
            for y in 0..dimensions.1 {
                for x in 0..dimensions.0 {
                    let idx = (z * dimensions.1 * dimensions.0 + y * dimensions.0 + x) as usize;
                    if idx < voxel_data.len() && voxel_data[idx] > 0 {
                        points.push([x as f32, y as f32, z as f32]);
                    }
                }
            }
        }
        
        self.embed_point_cloud(&points)
    }

    pub fn get_dimension(&self) -> usize {
        self.dimension
    }

    fn normalize(&self, vector: &mut Vec<f32>) {
        let norm: f32 = vector.iter().map(|x| x * x).sum::<f32>().sqrt();
        if norm > 0.0 {
            for v in vector.iter_mut() {
                *v /= norm;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_embed_point_cloud() {
        let service = PointCloudEmbeddingService::with_defaults();
        let points = vec![
            [0.0, 0.0, 0.0],
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
        ];
        
        let embedding = service.embed_point_cloud(&points).unwrap();
        
        assert_eq!(embedding.len(), 1024);
    }

    #[test]
    fn test_embed_point_cloud_empty() {
        let service = PointCloudEmbeddingService::with_defaults();
        let points: Vec<[f32; 3]> = vec![];
        
        let embedding = service.embed_point_cloud(&points).unwrap();
        
        assert_eq!(embedding.len(), 1024);
    }
}
