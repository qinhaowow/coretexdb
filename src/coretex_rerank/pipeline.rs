//! Multi-Stage Search Pipeline
//! Orchestrates coarse ranking and fine ranking stages

use crate::coretex_hybrid::{HybridQuery, MultiModalResult, FusedResult};
use crate::coretex_rerank::coarse_ranker::{CoarseRanker, CoarseResult, CoarseRankerConfig};
use crate::coretex_rerank::fine_ranker::{FineRanker, FineRankerConfig, RerankDocument};
use std::collections::HashMap;

pub struct TwoStageSearchPipeline {
    coarse_ranker: CoarseRanker,
    fine_ranker: FineRanker,
}

impl TwoStageSearchPipeline {
    pub fn new() -> Self {
        Self {
            coarse_ranker: CoarseRanker::with_default_config(),
            fine_ranker: FineRanker::new(FineRankerConfig::default()),
        }
    }

    pub fn with_coarse_config(mut self, config: CoarseRankerConfig) -> Self {
        self.coarse_ranker = CoarseRanker::new(config);
        self
    }

    pub fn with_fine_config(mut self, config: FineRankerConfig) -> Self {
        self.fine_ranker = FineRanker::new(config);
        self
    }

    /// Two-stage search with **synthesised** document bodies (`"doc {id}"`).
    ///
    /// Kept for callers without a document store; anything that has real
    /// texts should use [`Self::search_with_documents`] or
    /// [`Self::search_with_callback`] instead — term overlap against a
    /// synthetic body is meaningless.
    pub fn search(
        &mut self,
        query: &HybridQuery,
        raw_results: Vec<MultiModalResult>,
    ) -> Vec<FusedResult> {
        self.search_with_documents(query, raw_results, HashMap::new())
    }

    /// Two-stage search over `raw_results` with caller-supplied document
    /// texts: the coarse stage min-max normalises each source's scores, the
    /// fine stage scores `documents[id].text` against the query. Candidate
    /// ids missing from `documents` get the synthetic fallback body, so a
    /// partial map degrades instead of dropping hits.
    pub fn search_with_documents(
        &mut self,
        query: &HybridQuery,
        raw_results: Vec<MultiModalResult>,
        mut documents: HashMap<String, RerankDocument>,
    ) -> Vec<FusedResult> {
        let coarse_results: Vec<CoarseResult> = raw_results
            .iter()
            .map(|r| CoarseResult {
                id: r.id.clone(),
                source: r.source.clone(),
                raw_score: r.score,
                normalized_score: 0.0,
            })
            .collect();

        let coarse_ranked = self.coarse_ranker.rank(coarse_results);

        for r in &raw_results {
            documents
                .entry(r.id.clone())
                .or_insert_with(|| RerankDocument {
                    id: r.id.clone(),
                    text: format!("doc {}", r.id),
                    vector: None,
                });
        }

        let fine_ranked = self.fine_ranker.rerank(
            query
                .text_query
                .as_ref()
                .map(|t| t.query.as_str())
                .unwrap_or(""),
            &coarse_ranked,
            &documents,
        );

        fine_ranked
            .into_iter()
            .map(|r| FusedResult {
                id: r.id,
                score: r.final_score,
                sources: vec!["hybrid".to_string()],
            })
            .collect()
    }

    /// Two-stage search where document bodies come from `fetch_docs`,
    /// called once per candidate id **in candidate order**. Ids the
    /// callback declines fall back to the synthetic body (they are ranked,
    /// just cannot win on term overlap).
    pub fn search_with_callback<F>(
        &mut self,
        query: &HybridQuery,
        raw_results: Vec<MultiModalResult>,
        mut fetch_docs: F,
    ) -> Vec<FusedResult>
    where
        F: FnMut(&str) -> Option<RerankDocument>,
    {
        let mut documents = HashMap::new();
        for r in &raw_results {
            if let Some(doc) = fetch_docs(&r.id) {
                documents.insert(r.id.clone(), doc);
            }
        }
        self.search_with_documents(query, raw_results, documents)
    }
}

impl Default for TwoStageSearchPipeline {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(id: &str, source: &str, score: f32) -> MultiModalResult {
        MultiModalResult {
            id: id.to_string(),
            score,
            rank: 0,
            source: source.to_string(),
            weight: 1.0,
            metadata: None,
        }
    }

    fn doc(id: &str, text: &str) -> RerankDocument {
        RerankDocument {
            id: id.to_string(),
            text: text.to_string(),
            vector: None,
        }
    }

    #[test]
    fn test_two_stage_pipeline() {
        let mut pipeline = TwoStageSearchPipeline::new();

        let query = HybridQuery::new()
            .with_text("test query")
            .with_top_k(10);

        let results = pipeline.search(&query, vec![]);

        assert!(results.is_empty());
    }

    /// 粗排同分时，真实文档的词重叠必须决定顺序（B2 rerank 的核心语义）。
    #[test]
    fn document_terms_outrank_when_coarse_ties() {
        let mut pipeline = TwoStageSearchPipeline::new();
        let query = HybridQuery::new().with_text("alpha gamma").with_top_k(10);
        let raw = vec![
            candidate("a", "hybrid", 0.5),
            candidate("b", "hybrid", 0.5),
        ];
        let mut documents = HashMap::new();
        documents.insert("a".to_string(), doc("a", "alpha only"));
        documents.insert("b".to_string(), doc("b", "alpha gamma"));

        let out = pipeline.search_with_documents(&query, raw, documents);

        assert_eq!(out.len(), 2);
        assert_eq!(
            out[0].id, "b",
            "全部查询词命中的 b 必须胜过只命中一半的 a: {out:?}"
        );
        assert!(out[0].score > out[1].score);
    }

    /// 回调必须对每个候选真实调用；未提供文档的候选降级为合成文本而不是被丢弃。
    #[test]
    fn callback_fetches_real_candidates_and_degrades_on_miss() {
        let mut pipeline = TwoStageSearchPipeline::new();
        let query = HybridQuery::new().with_text("alpha").with_top_k(10);
        let raw = vec![
            candidate("real", "hybrid", 0.5),
            candidate("missing", "hybrid", 0.5),
        ];
        let mut asked: Vec<String> = Vec::new();

        let out = pipeline.search_with_callback(&query, raw, |id| {
            asked.push(id.to_string());
            (id == "real").then(|| doc(id, "alpha release"))
        });

        assert_eq!(asked, ["real", "missing"], "每个候选都必须被询问");
        assert_eq!(out.len(), 2, "miss 的候选不得被丢弃");
        assert_eq!(out[0].id, "real", "真实命中文档必须排前");
    }

    /// 空候选 → 空输出（不 panic）。
    #[test]
    fn empty_candidates_yield_empty_output() {
        let mut pipeline = TwoStageSearchPipeline::new();
        let query = HybridQuery::new().with_text("anything").with_top_k(5);
        let out = pipeline.search_with_callback(&query, vec![], |_| None);
        assert!(out.is_empty());
    }
}
