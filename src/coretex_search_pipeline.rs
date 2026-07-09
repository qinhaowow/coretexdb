//! 多模态与搜索增强：
//! 1. 中英文分词器 + 停用词 + 词干提取
//! 2. RRF（Reciprocal Rank Fusion）混合检索融合
//! 3. 多阶段 Rerank 管道
//! 4. Embedding 路由器（按数据/查询选择模型）
//! 5. 跨模态检索（文本→向量、图像→向量互查）

use std::collections::{HashMap, HashSet, BTreeMap};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

// =================== 文本预处理 ===================

/// 停用词表（中英文）
pub struct StopWords {
    english: HashSet<&'static str>,
    chinese: HashSet<&'static str>,
}

impl Default for StopWords {
    fn default() -> Self {
        Self::new()
    }
}

impl StopWords {
    pub fn new() -> Self {
        let english: HashSet<&'static str> = [
            "a", "an", "the", "is", "are", "was", "were", "be", "been", "being",
            "have", "has", "had", "do", "does", "did", "will", "would", "could", "should",
            "may", "might", "must", "shall", "can", "need", "dare", "ought", "used",
            "to", "of", "in", "for", "on", "with", "at", "by", "from", "as",
            "into", "through", "during", "before", "after", "above", "below",
            "between", "out", "against", "about", "i", "me", "my", "myself",
            "we", "our", "ours", "ourselves", "you", "your", "yours",
            "he", "him", "his", "she", "her", "it", "its", "they", "them",
            "their", "what", "which", "who", "whom", "this", "that", "these",
            "those", "and", "but", "if", "or", "because", "until", "while",
            "than", "so", "such", "no", "not", "only", "same", "very", "just",
        ].iter().copied().collect();

        let chinese: HashSet<&'static str> = [
            "的", "了", "是", "在", "和", "与", "或", "但", "而", "等",
            "我", "你", "他", "她", "它", "们", "这", "那", "些", "什么",
            "怎么", "如何", "为什么", "哪", "里", "上", "下", "前", "后",
            "中", "内", "外", "对", "为", "从", "到", "以", "把", "被",
            "有", "没", "也", "都", "就", "才", "还", "又", "再", "已",
        ].iter().copied().collect();

        Self { english, chinese }
    }

    pub fn is_stop(&self, word: &str) -> bool {
        self.english.contains(word) || self.chinese.contains(word)
    }
}

/// Porter 词干提取（简化版）
pub struct Stemmer;

impl Stemmer {
    /// 简单后缀剥离（不替代真正的 Porter 算法，作为 fallback）
    pub fn stem(word: &str) -> String {
        let w = word.to_lowercase();
        if w.len() < 4 {
            return w;
        }

        // 常见英语后缀
        let suffixes = ["ational", "tional", "alize", "icate", "ative", "ization",
                        "ation", "fulness", "ousness", "iveness", "ment", "ness",
                        "ing", "ies", "ied", "ly", "ed", "er", "est", "s"];

        for suffix in &suffixes {
            if w.ends_with(suffix) && w.len() > suffix.len() + 2 {
                return w[..w.len() - suffix.len()].to_string();
            }
        }

        w
    }
}

/// 文本分词器（中英文混合）
pub struct TextTokenizer {
    stop_words: StopWords,
    use_stemming: bool,
    min_token_length: usize,
    max_token_length: usize,
}

impl Default for TextTokenizer {
    fn default() -> Self {
        Self {
            stop_words: StopWords::new(),
            use_stemming: true,
            min_token_length: 1,
            max_token_length: 50,
        }
    }
}

impl TextTokenizer {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_stemming(mut self, use_stemming: bool) -> Self {
        self.use_stemming = use_stemming;
        self
    }

    /// 智能分词：中英文 + 数字
    pub fn tokenize(&self, text: &str) -> Vec<String> {
        let mut tokens = Vec::new();
        let mut current = String::new();
        let mut current_is_chinese = false;

        for ch in text.chars() {
            if ch.is_whitespace() {
                self.flush_token(&mut current, &mut tokens, current_is_chinese);
            } else if Self::is_chinese_char(ch) {
                // 中文：每个字单独作为 token（粗粒度分词的 fallback）
                self.flush_token(&mut current, &mut tokens, current_is_chinese);
                current.push(ch);
                current_is_chinese = true;
                self.flush_token(&mut current, &mut tokens, current_is_chinese);
            } else if ch.is_alphanumeric() || ch == '_' {
                if current_is_chinese {
                    self.flush_token(&mut current, &mut tokens, current_is_chinese);
                }
                current.push(ch.to_ascii_lowercase());
                current_is_chinese = false;
            } else {
                // 标点符号等
                self.flush_token(&mut current, &mut tokens, current_is_chinese);
            }
        }
        self.flush_token(&mut current, &mut tokens, current_is_chinese);

        // 过滤停用词
        tokens.into_iter()
            .filter(|t| {
                let t_len = t.chars().count();
                t_len >= self.min_token_length
                    && t_len <= self.max_token_length
                    && !self.stop_words.is_stop(t)
            })
            .map(|t| {
                if self.use_stemming && t.chars().all(|c| c.is_ascii_alphabetic()) {
                    Stemmer::stem(&t)
                } else {
                    t
                }
            })
            .collect()
    }

    fn flush_token(&self, current: &mut String, tokens: &mut Vec<String>, was_chinese: bool) {
        if !current.is_empty() {
            tokens.push(current.clone());
            current.clear();
        }
        let _ = was_chinese;
    }

    fn is_chinese_char(ch: char) -> bool {
        matches!(ch as u32, 0x4E00..=0x9FFF)
    }
}

// =================== RRF 混合检索融合 ===================

/// RRF（Reciprocal Rank Fusion）融合器
pub struct RRFFusion {
    /// RRF 参数 k（默认 60）
    pub k: f32,
}

impl Default for RRFFusion {
    fn default() -> Self {
        Self { k: 60.0 }
    }
}

impl RRFFusion {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_k(mut self, k: f32) -> Self {
        self.k = k;
        self
    }

    /// 融合多个排序结果
    /// inputs: 每个源的 (id, score) 列表（已排序）
    pub fn fuse(&self, inputs: &[Vec<(String, f32)>]) -> Vec<(String, f32)> {
        let mut scores: HashMap<String, f32> = HashMap::new();

        for input in inputs {
            for (rank, (id, _orig_score)) in input.iter().enumerate() {
                let rrf_score = 1.0 / (self.k + rank as f32 + 1.0);
                *scores.entry(id.clone()).or_default() += rrf_score;
            }
        }

        let mut result: Vec<(String, f32)> = scores.into_iter().collect();
        result.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        result
    }
}

// =================== 多阶段 Rerank 管道 ===================

/// 候选文档
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candidate {
    pub id: String,
    pub content: String,
    pub initial_score: f32,
    pub metadata: HashMap<String, String>,
}

/// Rerank 评分器 trait
#[async_trait::async_trait]
pub trait RerankScorer: Send + Sync {
    async fn score(&self, query: &str, candidate: &Candidate) -> f32;
}

/// 简单 BM25 重打分器（基于查询-文档 token 重叠）
pub struct BM25RerankScorer {
    k1: f32,
    b: f32,
}

impl BM25RerankScorer {
    pub fn new(k1: f32, b: f32) -> Self {
        Self { k1, b }
    }
}

#[async_trait::async_trait]
impl RerankScorer for BM25RerankScorer {
    async fn score(&self, query: &str, candidate: &Candidate) -> f32 {
        let tokenizer = TextTokenizer::new();
        let query_tokens = tokenizer.tokenize(query);
        let doc_tokens = tokenizer.tokenize(&candidate.content);
        let doc_len = doc_tokens.len() as f32;

        if doc_len == 0.0 {
            return 0.0;
        }

        let avgdl = 100.0; // 简化：固定平均长度
        let mut score = 0.0;
        let query_set: HashSet<&String> = query_tokens.iter().collect();

        for qt in &query_tokens {
            if !query_set.contains(qt) {
                continue;
            }
            let tf = doc_tokens.iter().filter(|t| *t == qt).count() as f32;
            if tf == 0.0 { continue; }
            let idf = 1.0; // 简化
            let norm = 1.0 - self.b + self.b * (doc_len / avgdl);
            score += idf * (tf * (self.k1 + 1.0)) / (tf + self.k1 * norm);
        }
        score
    }
}

/// 长度惩罚重打分器（避免返回过长片段）
pub struct LengthPenaltyScorer {
    /// 理想长度（字符数）
    pub ideal_length: usize,
    /// 惩罚系数
    pub penalty: f32,
}

#[async_trait::async_trait]
impl RerankScorer for LengthPenaltyScorer {
    async fn score(&self, _query: &str, candidate: &Candidate) -> f32 {
        let len = candidate.content.chars().count();
        let diff = (len as f32 - self.ideal_length as f32).abs();
        1.0 - (diff / self.ideal_length as f32) * self.penalty
    }
}

/// 多阶段 Rerank 管道
pub struct RerankPipeline {
    stages: Vec<Box<dyn RerankScorer>>,
    /// 每阶段保留 top N（0 = 不限）
    pub stage_top_n: Vec<usize>,
}

impl RerankPipeline {
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            stage_top_n: Vec::new(),
        }
    }

    pub fn add_stage<S: RerankScorer + 'static>(mut self, scorer: S, top_n: usize) -> Self {
        self.stages.push(Box::new(scorer));
        self.stage_top_n.push(top_n);
        self
    }

    /// 执行多阶段 Rerank
    pub async fn rerank(&self, query: &str, mut candidates: Vec<Candidate>) -> Vec<Candidate> {
        for (stage_idx, scorer) in self.stages.iter().enumerate() {
            // 打分
            for c in &mut candidates {
                let s = scorer.score(query, c).await;
                c.initial_score = c.initial_score * 0.5 + s * 0.5;
            }
            // 排序
            candidates.sort_by(|a, b| b.initial_score.partial_cmp(&a.initial_score).unwrap_or(std::cmp::Ordering::Equal));

            // 截断
            let top_n = self.stage_top_n.get(stage_idx).copied().unwrap_or(0);
            if top_n > 0 && candidates.len() > top_n {
                candidates.truncate(top_n);
            }
        }
        candidates
    }
}

impl Default for RerankPipeline {
    fn default() -> Self {
        Self::new()
    }
}

// =================== Embedding 路由器 ===================

/// 模态类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Modality {
    Text,
    Image,
    Audio,
    Video,
    PointCloud,
}

/// Embedding 模型元信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingModel {
    pub name: String,
    pub modality: Modality,
    pub dimension: usize,
    /// 每次推理成本（虚拟单位）
    pub cost: f32,
    /// 平均延迟 ms
    pub latency_ms: u32,
    /// 支持的语言（仅文本）
    pub languages: Vec<String>,
    /// 最大输入长度
    pub max_input_length: usize,
}

/// 路由策略
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RoutingStrategy {
    /// 成本最低
    LowestCost,
    /// 延迟最低
    LowestLatency,
    /// 能力匹配（按模态/语言）
    CapabilityMatch,
    /// 加权混合
    Weighted,
}

/// Embedding 路由器
pub struct EmbeddingRouter {
    models: Vec<EmbeddingModel>,
    strategy: RoutingStrategy,
    weights: RoutingWeights,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingWeights {
    pub cost: f32,
    pub latency: f32,
    pub capability: f32,
}

impl Default for RoutingWeights {
    fn default() -> Self {
        Self { cost: 0.3, latency: 0.3, capability: 0.4 }
    }
}

impl EmbeddingRouter {
    pub fn new(strategy: RoutingStrategy) -> Self {
        Self {
            models: Vec::new(),
            strategy,
            weights: RoutingWeights::default(),
        }
    }

    pub fn register_model(&mut self, model: EmbeddingModel) {
        self.models.push(model);
    }

    /// 根据查询路由到最合适的模型
    pub fn route(&self, modality: Modality, query: &str) -> Option<&EmbeddingModel> {
        let candidates: Vec<&EmbeddingModel> = self.models
            .iter()
            .filter(|m| m.modality == modality)
            .collect();

        if candidates.is_empty() {
            return None;
        }

        match self.strategy {
            RoutingStrategy::LowestCost => {
                candidates.into_iter().min_by(|a, b| a.cost.partial_cmp(&b.cost).unwrap_or(std::cmp::Ordering::Equal))
            }
            RoutingStrategy::LowestLatency => {
                candidates.into_iter().min_by_key(|m| m.latency_ms)
            }
            RoutingStrategy::CapabilityMatch => {
                // 简单能力匹配：检查查询语言
                let is_chinese = query.chars().any(|c| (c as u32) >= 0x4E00 && (c as u32) <= 0x9FFF);
                let target_lang = if is_chinese { "zh" } else { "en" };
                candidates.into_iter()
                    .find(|m| m.languages.contains(&target_lang.to_string()))
                    .or_else(|| candidates.into_iter().next())
            }
            RoutingStrategy::Weighted => {
                // 加权评分：score = w_capability * cap_score + w_cost * (1-cost/max) + w_latency * (1-latency/max)
                let max_cost = candidates.iter().map(|m| m.cost).fold(0.0_f32, f32::max).max(1.0);
                let max_latency = candidates.iter().map(|m| m.latency_ms).max().unwrap_or(1) as f32;
                let is_chinese = query.chars().any(|c| (c as u32) >= 0x4E00 && (c as u32) <= 0x9FFF);
                let target_lang = if is_chinese { "zh" } else { "en" };

                let mut best: Option<&EmbeddingModel> = None;
                let mut best_score = f32::MIN;

                for m in candidates {
                    let cap_score = if m.languages.contains(&target_lang.to_string()) { 1.0 } else { 0.5 };
                    let cost_score = 1.0 - (m.cost / max_cost);
                    let latency_score = 1.0 - (m.latency_ms as f32 / max_latency);
                    let score = self.weights.capability * cap_score
                        + self.weights.cost * cost_score
                        + self.weights.latency * latency_score;
                    if score > best_score {
                        best_score = score;
                        best = Some(m);
                    }
                }
                best
            }
        }
    }
}

// =================== 跨模态检索 ===================

/// 跨模态检索结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossModalResult {
    pub id: String,
    pub source_modality: Modality,
    pub similarity: f32,
    pub content: String,
}

/// 跨模态检索器
pub struct CrossModalRetriever {
    /// 各模态的 embedding 索引（共享向量空间）
    modality_indexes: Arc<RwLock<HashMap<Modality, HashMap<String, (Vec<f32>, String)>>>>,
}

impl Default for CrossModalRetriever {
    fn default() -> Self {
        Self::new()
    }
}

impl CrossModalRetriever {
    pub fn new() -> Self {
        Self {
            modality_indexes: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn insert(&self, id: String, modality: Modality, embedding: Vec<f32>, content: String) {
        let mut indexes = self.modality_indexes.write().await;
        indexes.entry(modality).or_default().insert(id, (embedding, content));
    }

    /// 跨模态搜索：以一个模态的查询向量，搜索所有模态
    pub async fn cross_search(
        &self,
        query_vec: &[f32],
        k: usize,
    ) -> Vec<CrossModalResult> {
        let indexes = self.modality_indexes.read().await;
        let mut results = Vec::new();

        for (&modality, map) in indexes.iter() {
            for (id, (emb, content)) in map {
                if emb.len() != query_vec.len() {
                    continue;
                }
                let similarity = cosine_similarity(query_vec, emb);
                results.push(CrossModalResult {
                    id: id.clone(),
                    source_modality: modality,
                    similarity,
                    content: content.clone(),
                });
            }
        }

        results.sort_by(|a, b| b.similarity.partial_cmp(&a.similarity).unwrap_or(std::cmp::Ordering::Equal));
        results.truncate(k);
        results
    }
}

fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let na: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let nb: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();
    if na == 0.0 || nb == 0.0 { return 0.0; }
    dot / (na * nb)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_text_tokenizer_chinese_english() {
        let t = TextTokenizer::new();
        let tokens = t.tokenize("Hello World 你好世界 Rust async");
        assert!(tokens.contains(&"hello".to_string()));
        assert!(tokens.contains(&"world".to_string()));
        assert!(tokens.iter().any(|t| t == "rust" || t == "rust"));
        // 停用词应被过滤
        assert!(!tokens.contains(&"the".to_string()));
    }

    #[test]
    fn test_text_tokenizer_filters_stopwords() {
        let t = TextTokenizer::new();
        let tokens = t.tokenize("the quick brown fox");
        assert!(!tokens.contains(&"the".to_string()));
        assert!(tokens.contains(&"quick".to_string()));
    }

    #[test]
    fn test_stemmer_basic() {
        assert_eq!(Stemmer::stem("running"), "runn");
        assert_eq!(Stemmer::stem("connections"), "connection");
    }

    #[test]
    fn test_rrf_fusion() {
        let rrf = RRFFusion::new();
        let list1 = vec![("a".to_string(), 0.9), ("b".to_string(), 0.7), ("c".to_string(), 0.5)];
        let list2 = vec![("b".to_string(), 0.95), ("d".to_string(), 0.6), ("a".to_string(), 0.3)];
        let fused = rrf.fuse(&[list1, list2]);
        // b 在两个列表都靠前，应该得分最高
        assert_eq!(fused[0].0, "b");
    }

    #[tokio::test]
    async fn test_rerank_pipeline() {
        let pipeline = RerankPipeline::new()
            .add_stage(BM25RerankScorer::new(1.2, 0.75), 10)
            .add_stage(LengthPenaltyScorer { ideal_length: 100, penalty: 0.5 }, 5);

        let candidates = vec![
            Candidate {
                id: "1".to_string(),
                content: "the quick brown fox".to_string(),
                initial_score: 0.5,
                metadata: HashMap::new(),
            },
            Candidate {
                id: "2".to_string(),
                content: "the quick red fox".to_string(),
                initial_score: 0.6,
                metadata: HashMap::new(),
            },
        ];
        let reranked = pipeline.rerank("quick fox", candidates).await;
        assert!(!reranked.is_empty());
    }

    #[test]
    fn test_embedding_router_lowest_cost() {
        let mut router = EmbeddingRouter::new(RoutingStrategy::LowestCost);
        router.register_model(EmbeddingModel {
            name: "expensive".to_string(),
            modality: Modality::Text,
            dimension: 768,
            cost: 10.0,
            latency_ms: 100,
            languages: vec!["en".to_string()],
            max_input_length: 512,
        });
        router.register_model(EmbeddingModel {
            name: "cheap".to_string(),
            modality: Modality::Text,
            dimension: 384,
            cost: 1.0,
            latency_ms: 50,
            languages: vec!["en".to_string()],
            max_input_length: 256,
        });
        let model = router.route(Modality::Text, "hello").unwrap();
        assert_eq!(model.name, "cheap");
    }

    #[test]
    fn test_embedding_router_capability_match() {
        let mut router = EmbeddingRouter::new(RoutingStrategy::CapabilityMatch);
        router.register_model(EmbeddingModel {
            name: "english_only".to_string(),
            modality: Modality::Text,
            dimension: 768,
            cost: 1.0,
            latency_ms: 50,
            languages: vec!["en".to_string()],
            max_input_length: 512,
        });
        router.register_model(EmbeddingModel {
            name: "multilingual".to_string(),
            modality: Modality::Text,
            dimension: 768,
            cost: 1.0,
            latency_ms: 50,
            languages: vec!["en".to_string(), "zh".to_string()],
            max_input_length: 512,
        });
        let model = router.route(Modality::Text, "你好世界").unwrap();
        assert_eq!(model.name, "multilingual");
    }

    #[tokio::test]
    async fn test_cross_modal_search() {
        let retriever = CrossModalRetriever::new();
        retriever.insert("img1".to_string(), Modality::Image, vec![0.1, 0.2, 0.3], "cat.jpg".to_string()).await;
        retriever.insert("img2".to_string(), Modality::Image, vec![0.4, 0.5, 0.6], "dog.jpg".to_string()).await;
        retriever.insert("txt1".to_string(), Modality::Text, vec![0.1, 0.2, 0.3], "a cat sitting".to_string()).await;

        let results = retriever.cross_search(&[0.1, 0.2, 0.3], 3).await;
        // img1 和 txt1 与查询向量相同，应排在前
        assert_eq!(results[0].similarity, 1.0);
    }
}
