use std::collections::HashMap;
use ndarray::Array1;
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GRPOConfig {
    pub learning_rate: f64,
    pub clip_epsilon: f64,
    pub kl_coefficient: f64,
    pub group_size: usize,
    pub top_k_groups: usize,
    pub entropy_coefficient: f64,
    pub max_grad_norm: f64,
    pub update_steps: usize,
    pub discount_factor: f64,
    pub gae_lambda: f64,
}

impl Default for GRPOConfig {
    fn default() -> Self {
        Self {
            learning_rate: 0.001,
            clip_epsilon: 0.2,
            kl_coefficient: 0.01,
            group_size: 8,
            top_k_groups: 4,
            entropy_coefficient: 0.01,
            max_grad_norm: 1.0,
            update_steps: 5,
            discount_factor: 0.99,
            gae_lambda: 0.95,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolicyNetwork {
    #[serde(skip)]
    weights: Array1<f64>,
    input_dim: usize,
    output_dim: usize,
    hidden_dim: usize,
    layer1_weights: Vec<Vec<f64>>,
    layer1_bias: Vec<f64>,
    layer2_weights: Vec<Vec<f64>>,
    layer2_bias: Vec<f64>,
}

impl PolicyNetwork {
    pub fn new(input_dim: usize, output_dim: usize, hidden_dim: usize) -> Self {
        let mut rng = rand::thread_rng();

        let layer1_weights = (0..hidden_dim)
            .map(|_| (0..input_dim).map(|_| rng.gen::<f64>() * 0.1 - 0.05).collect())
            .collect();
        let layer1_bias = (0..hidden_dim).map(|_| 0.0).collect();

        let layer2_weights = (0..output_dim)
            .map(|_| (0..hidden_dim).map(|_| rng.gen::<f64>() * 0.1 - 0.05).collect())
            .collect();
        let layer2_bias = (0..output_dim).map(|_| 0.0).collect();

        Self {
            weights: Array1::zeros(input_dim * output_dim),
            input_dim,
            output_dim,
            hidden_dim,
            layer1_weights,
            layer1_bias,
            layer2_weights,
            layer2_bias,
        }
    }

    pub fn forward(&self, input: &[f64]) -> Vec<f64> {
        let hidden: Vec<f64> = self
            .layer1_weights
            .iter()
            .zip(self.layer1_bias.iter())
            .map(|(weights, bias)| {
                let sum: f64 = weights.iter().zip(input.iter()).map(|(w, x)| w * x).sum();
                (sum + bias).tanh()
            })
            .collect();

        let logits: Vec<f64> = self
            .layer2_weights
            .iter()
            .zip(self.layer2_bias.iter())
            .map(|(weights, bias)| {
                let sum: f64 = weights.iter().zip(hidden.iter()).map(|(w, h)| w * h).sum();
                sum + bias
            })
            .collect();

        softmax(&logits)
    }

    pub fn get_params_count(&self) -> usize {
        self.input_dim * self.hidden_dim
            + self.hidden_dim
            + self.hidden_dim * self.output_dim
            + self.output_dim
    }
}

fn softmax(logits: &[f64]) -> Vec<f64> {
    let max_val = logits.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let exps: Vec<f64> = logits.iter().map(|x| (x - max_val).exp()).collect();
    let sum: f64 = exps.iter().sum();
    exps.iter().map(|x| x / sum).collect()
}

fn kl_divergence(p: &[f64], q: &[f64]) -> f64 {
    p.iter()
        .zip(q.iter())
        .filter(|(pi, _)| *pi > &1e-10)
        .map(|(pi, qi)| pi * (pi / qi).ln())
        .sum()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GRPOExperience {
    pub state: Vec<f64>,
    pub action: usize,
    pub old_log_prob: f64,
    pub reward: f64,
    pub group_id: usize,
    pub value: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GRPOStats {
    pub total_updates: u64,
    pub average_reward: f64,
    pub best_reward: f64,
    pub policy_loss: f64,
    pub kl_divergence: f64,
    pub entropy: f64,
    pub learning_progress: f64,
}

pub struct GRPOOptimizer {
    config: GRPOConfig,
    policy: PolicyNetwork,
    old_policy: PolicyNetwork,
    experiences: Vec<GRPOExperience>,
    stats: GRPOStats,
    rng: rand::rngs::ThreadRng,
}

impl GRPOOptimizer {
    pub fn new(config: GRPOConfig, input_dim: usize, output_dim: usize) -> Self {
        let policy = PolicyNetwork::new(input_dim, output_dim, 64);
        let old_policy = PolicyNetwork::new(input_dim, output_dim, 64);

        Self {
            stats: GRPOStats {
                total_updates: 0,
                average_reward: 0.0,
                best_reward: f64::NEG_INFINITY,
                policy_loss: 0.0,
                kl_divergence: 0.0,
                entropy: 0.0,
                learning_progress: 0.0,
            },
            config,
            policy,
            old_policy,
            experiences: Vec::new(),
            rng: rand::thread_rng(),
        }
    }

    pub fn select_action(&mut self, state: &[f64]) -> (usize, f64) {
        let probs = self.policy.forward(state);
        let action = sample_from_probs(&probs, &mut self.rng);
        let log_prob = probs[action].ln().max(-20.0);
        (action, log_prob)
    }

    pub fn add_experience(&mut self, experience: GRPOExperience) {
        self.experiences.push(experience);
    }

    pub fn compute_group_advantages(&self, group_id: usize) -> Vec<f64> {
        let group_rewards: Vec<f64> = self
            .experiences
            .iter()
            .filter(|e| e.group_id == group_id)
            .map(|e| e.reward)
            .collect();

        if group_rewards.is_empty() {
            return Vec::new();
        }

        let mean: f64 = group_rewards.iter().sum::<f64>() / group_rewards.len() as f64;
        let std: f64 = if group_rewards.len() > 1 {
            let variance: f64 = group_rewards
                .iter()
                .map(|r| (r - mean).powi(2))
                .sum::<f64>()
                / group_rewards.len() as f64;
            variance.sqrt().max(1e-8)
        } else {
            1.0
        };

        group_rewards.iter().map(|r| (r - mean) / std).collect()
    }

    pub fn update_policy(&mut self) -> GRPOUpdateResult {
        if self.experiences.is_empty() {
            return GRPOUpdateResult {
                policy_loss: 0.0,
                kl_loss: 0.0,
                entropy_loss: 0.0,
                total_loss: 0.0,
                kl_divergence: 0.0,
                average_advantage: 0.0,
            };
        }

        self.old_policy = PolicyNetwork::new(
            self.policy.input_dim,
            self.policy.output_dim,
            self.policy.hidden_dim,
        );
        self.old_policy.layer1_weights = self.policy.layer1_weights.clone();
        self.old_policy.layer1_bias = self.policy.layer1_bias.clone();
        self.old_policy.layer2_weights = self.policy.layer2_weights.clone();
        self.old_policy.layer2_bias = self.policy.layer2_bias.clone();

        let mut total_policy_loss = 0.0;
        let mut total_kl_loss = 0.0;
        let mut total_entropy_loss = 0.0;
        let mut total_advantages = 0.0;

        let group_ids: Vec<usize> = {
            let mut ids: Vec<usize> = self.experiences.iter().map(|e| e.group_id).collect();
            ids.sort_unstable();
            ids.dedup();
            ids
        };

        for group_id in &group_ids {
            let advantages = self.compute_group_advantages(*group_id);
            let group_experiences: Vec<&GRPOExperience> = self
                .experiences
                .iter()
                .filter(|e| e.group_id == *group_id)
                .collect();

            for (exp, advantage) in group_experiences.iter().zip(advantages.iter()) {
                let new_probs = self.policy.forward(&exp.state);
                let new_log_prob = new_probs[exp.action].ln().max(-20.0);

                let ratio = (new_log_prob - exp.old_log_prob).exp();
                let clipped_ratio = ratio.clamp(
                    1.0 - self.config.clip_epsilon,
                    1.0 + self.config.clip_epsilon,
                );

                let policy_loss = -ratio.min(clipped_ratio) * advantage;
                total_policy_loss += policy_loss;

                let old_probs = self.old_policy.forward(&exp.state);
                let kl = kl_divergence(&old_probs, &new_probs);
                total_kl_loss += kl * self.config.kl_coefficient;

                let entropy: f64 = new_probs
                    .iter()
                    .map(|p| if *p > 1e-10 { -p * p.ln() } else { 0.0 })
                    .sum();
                total_entropy_loss -= entropy * self.config.entropy_coefficient;

                total_advantages += advantage;
            }
        }

        let count = self.experiences.len() as f64;
        let avg_policy_loss = total_policy_loss / count.max(1.0);
        let avg_kl_loss = total_kl_loss / count.max(1.0);
        let avg_entropy_loss = total_entropy_loss / count.max(1.0);
        let total_loss = avg_policy_loss + avg_kl_loss + avg_entropy_loss;
        let avg_advantage = total_advantages / count.max(1.0);

        let lr = self.config.learning_rate;
        for i in 0..self.policy.layer1_weights.len() {
            for j in 0..self.policy.layer1_weights[i].len() {
                let grad = self.policy.layer1_weights[i][j] * total_loss * 0.01;
                self.policy.layer1_weights[i][j] -= lr * grad.clamp(-1.0, 1.0);
            }
            let grad = self.policy.layer1_bias[i] * total_loss * 0.01;
            self.policy.layer1_bias[i] -= lr * grad.clamp(-1.0, 1.0);
        }

        for i in 0..self.policy.layer2_weights.len() {
            for j in 0..self.policy.layer2_weights[i].len() {
                let grad = self.policy.layer2_weights[i][j] * total_loss * 0.01;
                self.policy.layer2_weights[i][j] -= lr * grad.clamp(-1.0, 1.0);
            }
            let grad = self.policy.layer2_bias[i] * total_loss * 0.01;
            self.policy.layer2_bias[i] -= lr * grad.clamp(-1.0, 1.0);
        }

        let avg_reward: f64 = self.experiences.iter().map(|e| e.reward).sum::<f64>() / count.max(1.0);
        let best_reward = self
            .experiences
            .iter()
            .map(|e| e.reward)
            .fold(f64::NEG_INFINITY, f64::max);

        self.stats.total_updates += 1;
        self.stats.average_reward = avg_reward;
        self.stats.best_reward = self.stats.best_reward.max(best_reward);
        self.stats.policy_loss = avg_policy_loss;
        self.stats.kl_divergence = avg_kl_loss;
        self.stats.entropy = -avg_entropy_loss;
        self.stats.learning_progress = if self.stats.total_updates > 1 {
            (avg_reward - self.stats.average_reward).abs().min(1.0)
        } else {
            0.0
        };

        self.experiences.clear();

        GRPOUpdateResult {
            policy_loss: avg_policy_loss,
            kl_loss: avg_kl_loss,
            entropy_loss: avg_entropy_loss,
            total_loss,
            kl_divergence: avg_kl_loss,
            average_advantage: avg_advantage,
        }
    }

    pub fn get_stats(&self) -> &GRPOStats {
        &self.stats
    }

    pub fn get_policy(&self) -> &PolicyNetwork {
        &self.policy
    }

    pub fn config(&self) -> &GRPOConfig {
        &self.config
    }

    pub fn save_policy(&self) -> Vec<u8> {
        let policy_data = PolicyData {
            layer1_weights: self.policy.layer1_weights.clone(),
            layer1_bias: self.policy.layer1_bias.clone(),
            layer2_weights: self.policy.layer2_weights.clone(),
            layer2_bias: self.policy.layer2_bias.clone(),
            input_dim: self.policy.input_dim,
            output_dim: self.policy.output_dim,
            hidden_dim: self.policy.hidden_dim,
        };
        bincode::serialize(&policy_data).unwrap_or_default()
    }

    pub fn load_policy(&mut self, data: &[u8]) -> Result<(), String> {
        let policy_data: PolicyData = bincode::deserialize(data).map_err(|e| e.to_string())?;
        self.policy = PolicyNetwork {
            weights: Array1::zeros(policy_data.input_dim * policy_data.output_dim),
            input_dim: policy_data.input_dim,
            output_dim: policy_data.output_dim,
            hidden_dim: policy_data.hidden_dim,
            layer1_weights: policy_data.layer1_weights,
            layer1_bias: policy_data.layer1_bias,
            layer2_weights: policy_data.layer2_weights,
            layer2_bias: policy_data.layer2_bias,
        };
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PolicyData {
    layer1_weights: Vec<Vec<f64>>,
    layer1_bias: Vec<f64>,
    layer2_weights: Vec<Vec<f64>>,
    layer2_bias: Vec<f64>,
    input_dim: usize,
    output_dim: usize,
    hidden_dim: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GRPOUpdateResult {
    pub policy_loss: f64,
    pub kl_loss: f64,
    pub entropy_loss: f64,
    pub total_loss: f64,
    pub kl_divergence: f64,
    pub average_advantage: f64,
}

fn sample_from_probs(probs: &[f64], rng: &mut impl Rng) -> usize {
    let mut cumulative = 0.0;
    let sample: f64 = rng.gen();
    for (i, p) in probs.iter().enumerate() {
        cumulative += p;
        if sample <= cumulative {
            return i;
        }
    }
    probs.len() - 1
}

pub struct GRPOSearchOptimizer {
    grpo: GRPOOptimizer,
    feature_dim: usize,
    action_history: Vec<(Vec<f64>, usize, f64)>,
}

impl GRPOSearchOptimizer {
    pub fn new(feature_dim: usize, config: Option<GRPOConfig>) -> Self {
        let cfg = config.unwrap_or_default();
        Self {
            grpo: GRPOOptimizer::new(cfg, feature_dim, 10),
            feature_dim,
            action_history: Vec::new(),
        }
    }

    pub fn optimize_search_parameters(&mut self, query_features: &[f64]) -> SearchAction {
        let (action_idx, log_prob) = self.grpo.select_action(query_features);
        let action = SearchAction::from_index(action_idx);

        self.action_history
            .push((query_features.to_vec(), action_idx, log_prob));
        action
    }

    pub fn provide_feedback(&mut self, reward: f64, group_id: usize) {
        if let Some((state, action_idx, log_prob)) = self.action_history.pop() {
            self.grpo.add_experience(GRPOExperience {
                state,
                action: action_idx,
                old_log_prob: log_prob,
                reward,
                group_id,
                value: reward,
            });
        }
    }

    pub fn update(&mut self) -> GRPOUpdateResult {
        self.grpo.update_policy()
    }

    pub fn get_optimizer(&self) -> &GRPOOptimizer {
        &self.grpo
    }

    pub fn get_optimizer_mut(&mut self) -> &mut GRPOOptimizer {
        &mut self.grpo
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum SearchAction {
    TopK(usize),
    EFSearch(usize),
    EFConstruction(usize),
    M(usize),
    NProbe(usize),
    Alpha(f64),
    Lambda(f64),
    Weight(f64),
    FusionMethod(usize),
    Skip,
}

impl SearchAction {
    pub fn from_index(index: usize) -> Self {
        match index % 10 {
            0 => SearchAction::TopK(10),
            1 => SearchAction::EFSearch(100),
            2 => SearchAction::EFConstruction(200),
            3 => SearchAction::M(16),
            4 => SearchAction::NProbe(8),
            5 => SearchAction::Alpha(0.5),
            6 => SearchAction::Lambda(0.7),
            7 => SearchAction::Weight(0.3),
            8 => SearchAction::FusionMethod(1),
            _ => SearchAction::Skip,
        }
    }

    pub fn apply_to_params(&self, params: &mut HashMap<String, f64>) {
        match *self {
            SearchAction::TopK(k) => { params.insert("top_k".to_string(), k as f64); }
            SearchAction::EFSearch(ef) => { params.insert("ef_search".to_string(), ef as f64); }
            SearchAction::EFConstruction(efc) => { params.insert("ef_construction".to_string(), efc as f64); }
            SearchAction::M(m) => { params.insert("m".to_string(), m as f64); }
            SearchAction::NProbe(n) => { params.insert("nprobe".to_string(), n as f64); }
            SearchAction::Alpha(a) => { params.insert("alpha".to_string(), a); }
            SearchAction::Lambda(l) => { params.insert("lambda".to_string(), l); }
            SearchAction::Weight(w) => { params.insert("weight".to_string(), w); }
            SearchAction::FusionMethod(m) => { params.insert("fusion_method".to_string(), m as f64); }
            SearchAction::Skip => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpo_config_default() {
        let config = GRPOConfig::default();
        assert_eq!(config.learning_rate, 0.001);
        assert_eq!(config.clip_epsilon, 0.2);
        assert_eq!(config.group_size, 8);
    }

    #[test]
    fn test_policy_network_forward() {
        let network = PolicyNetwork::new(4, 3, 8);
        let input = vec![0.1, 0.2, 0.3, 0.4];
        let output = network.forward(&input);
        assert_eq!(output.len(), 3);
        let sum: f64 = output.iter().sum();
        assert!((sum - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_softmax() {
        let logits = vec![1.0, 2.0, 3.0];
        let probs = softmax(&logits);
        assert_eq!(probs.len(), 3);
        let sum: f64 = probs.iter().sum();
        assert!((sum - 1.0).abs() < 1e-6);
        assert!(probs[2] > probs[1]);
        assert!(probs[1] > probs[0]);
    }

    #[test]
    fn test_kl_divergence() {
        let p = vec![0.5, 0.3, 0.2];
        let q = vec![0.4, 0.4, 0.2];
        let kl = kl_divergence(&p, &q);
        assert!(kl > 0.0);
    }

    #[test]
    fn test_grpo_optimizer_new() {
        let config = GRPOConfig::default();
        let mut optimizer = GRPOOptimizer::new(config, 4, 3);
        let state = vec![0.1, 0.2, 0.3, 0.4];
        let (action, log_prob) = optimizer.select_action(&state);
        assert!(action < 3);
        assert!(log_prob < 0.0);
    }

    #[test]
    fn test_grpo_update() {
        let config = GRPOConfig {
            learning_rate: 0.01,
            clip_epsilon: 0.2,
            kl_coefficient: 0.01,
            group_size: 4,
            top_k_groups: 2,
            entropy_coefficient: 0.01,
            max_grad_norm: 1.0,
            update_steps: 3,
            discount_factor: 0.99,
            gae_lambda: 0.95,
        };
        let mut optimizer = GRPOOptimizer::new(config, 4, 3);

        for group_id in 0..2 {
            for _ in 0..4 {
                let state = vec![0.1, 0.2, 0.3, 0.4];
                let (action, log_prob) = optimizer.select_action(&state);
                optimizer.add_experience(GRPOExperience {
                    state,
                    action,
                    old_log_prob: log_prob,
                    reward: rand::random::<f64>() * 2.0 - 1.0,
                    group_id,
                    value: 0.0,
                });
            }
        }

        let result = optimizer.update_policy();
        assert!(result.total_loss.is_finite());
    }

    #[test]
    fn test_group_advantages() {
        let config = GRPOConfig::default();
        let mut optimizer = GRPOOptimizer::new(config, 4, 3);

        for i in 0..4 {
            optimizer.add_experience(GRPOExperience {
                state: vec![0.1, 0.2, 0.3, 0.4],
                action: 0,
                old_log_prob: -1.0,
                reward: i as f64,
                group_id: 0,
                value: 0.0,
            });
        }

        let advantages = optimizer.compute_group_advantages(0);
        assert_eq!(advantages.len(), 4);
        let sum: f64 = advantages.iter().sum();
        assert!((sum).abs() < 1e-6);
    }

    #[test]
    fn test_search_action() {
        let action = SearchAction::from_index(0);
        match action {
            SearchAction::TopK(k) => assert_eq!(k, 10),
            _ => panic!("Expected TopK"),
        }

        let mut params = HashMap::new();
        action.apply_to_params(&mut params);
        assert_eq!(params.get("top_k"), Some(&10.0));
    }

    #[test]
    fn test_search_optimizer() {
        let mut optimizer = GRPOSearchOptimizer::new(4, None);
        let features = vec![0.1, 0.2, 0.3, 0.4];
        let action = optimizer.optimize_search_parameters(&features);
        optimizer.provide_feedback(1.0, 0);
        let result = optimizer.update();
        assert!(result.total_loss.is_finite());
    }

    #[test]
    fn test_policy_save_load() {
        let config = GRPOConfig::default();
        let mut optimizer = GRPOOptimizer::new(config, 4, 3);
        let data = optimizer.save_policy();
        assert!(!data.is_empty());

        let mut new_optimizer = GRPOOptimizer::new(GRPOConfig::default(), 4, 3);
        assert!(new_optimizer.load_policy(&data).is_ok());
    }
}
