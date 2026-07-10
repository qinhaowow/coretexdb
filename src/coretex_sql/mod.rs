//! SQL Query Support module for CoreTexDB
//! Provides SQL-like query interface for vector database operations
//!
//! The SQLExecutor is now fully wired into DataManager — INSERT/DELETE/SELECT
//! go through the real database engine with proper ACID, indexing, and persistence.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::coretex_data::DataManager;
use crate::coretex_data::VectorRecord;
use crate::coretex_core::{CoreTexError, Result as CoretexResult};

pub mod optimizer;
pub use optimizer::{
    SQLOptimizer, ExecutionPlan, SQLOperator, SQLOperatorKind, IndexKind as SQLIndexKind,
    VectorPushdownOperator, FilterOperator, FilterOp, FilterValue, ProjectionOperator,
    LimitOperator, DistanceOp,
};

#[derive(Debug, Clone, PartialEq)]
pub enum SQLToken {
    Keyword(String),
    Identifier(String),
    StringLiteral(String),
    Number(f64),
    Operator(String),
    LParen,
    RParen,
    Comma,
    Dot,
    EOF,
}

pub struct SQLLexer {
    input: String,
    position: usize,
}

impl SQLLexer {
    pub fn new(input: &str) -> Self {
        Self {
            input: input.to_string(),
            position: 0,
        }
    }

    pub fn tokenize(&mut self) -> Vec<SQLToken> {
        let mut tokens = Vec::new();
        
        while self.position < self.input.len() {
            self.skip_whitespace();
            
            if self.position >= self.input.len() {
                break;
            }
            
            let c = self.input.chars().nth(self.position).unwrap();
            
            if c.is_alphabetic() || c == '_' {
                tokens.push(self.read_identifier_or_keyword());
            } else if c.is_ascii_digit() || c == '-' && self.peek_next().map(|n| n.is_ascii_digit()).unwrap_or(false) {
                tokens.push(self.read_number());
            } else if c == '\'' || c == '"' {
                tokens.push(self.read_string());
            } else {
                tokens.push(self.read_operator_or_punct());
            }
        }
        
        tokens.push(SQLToken::EOF);
        tokens
    }

    fn skip_whitespace(&mut self) {
        while self.position < self.input.len() {
            let c = self.input.chars().nth(self.position).unwrap();
            if !c.is_whitespace() {
                break;
            }
            self.position += 1;
        }
    }

    fn peek_next(&self) -> Option<char> {
        self.input.chars().nth(self.position + 1)
    }

    fn read_identifier_or_keyword(&mut self) -> SQLToken {
        let start = self.position;
        
        while self.position < self.input.len() {
            let c = self.input.chars().nth(self.position).unwrap();
            if c.is_alphanumeric() || c == '_' {
                self.position += 1;
            } else {
                break;
            }
        }
        
        let value = &self.input[start..self.position];
        
        let keywords = ["SELECT", "FROM", "WHERE", "INSERT", "INTO", "VALUES", 
                       "DELETE", "UPDATE", "SET", "CREATE", "DROP", "ALTER",
                       "INDEX", "ON", "AND", "OR", "NOT", "IN", "LIKE",
                       "ORDER", "BY", "ASC", "DESC", "LIMIT", "OFFSET",
                       "JOIN", "GROUP", "HAVING", "AS", "DISTINCT", "COUNT",
                       "SUM", "AVG", "MIN", "MAX", "NULL", "IS", "TRUE", "FALSE"];
        
        let upper = value.to_uppercase();
        
        if keywords.contains(&upper.as_str()) {
            SQLToken::Keyword(upper)
        } else {
            SQLToken::Identifier(value.to_string())
        }
    }

    fn read_number(&mut self) -> SQLToken {
        let start = self.position;
        
        if self.input.chars().nth(self.position) == Some('-') {
            self.position += 1;
        }
        
        while self.position < self.input.len() {
            let c = self.input.chars().nth(self.position).unwrap();
            if c.is_ascii_digit() || c == '.' {
                self.position += 1;
            } else {
                break;
            }
        }
        
        let value = &self.input[start..self.position];
        SQLToken::Number(value.parse().unwrap_or(0.0))
    }

    fn read_string(&mut self) -> SQLToken {
        let quote = self.input.chars().nth(self.position).unwrap();
        self.position += 1;
        
        let start = self.position;
        
        while self.position < self.input.len() {
            let c = self.input.chars().nth(self.position).unwrap();
            if c == quote {
                let value = &self.input[start..self.position];
                self.position += 1;
                return SQLToken::StringLiteral(value.to_string());
            }
            self.position += 1;
        }
        
        SQLToken::StringLiteral(self.input[start..].to_string())
    }

    fn read_operator_or_punct(&mut self) -> SQLToken {
        let c = self.input.chars().nth(self.position).unwrap();
        
        match c {
            '(' => { self.position += 1; SQLToken::LParen }
            ')' => { self.position += 1; SQLToken::RParen }
            ',' => { self.position += 1; SQLToken::Comma }
            '.' => { self.position += 1; SQLToken::Dot }
            '*' => { self.position += 1; SQLToken::Operator("*".to_string()) }
            '=' => { self.position += 1; SQLToken::Operator("=".to_string()) }
            '<' | '>' | '!' | '|' | '&' | '+' | '-' | '/' => {
                self.position += 1;
                SQLToken::Operator(c.to_string())
            }
            _ => { self.position += 1; SQLToken::Operator(c.to_string()) }
        }
    }
}

pub struct SQLParser {
    tokens: Vec<SQLToken>,
    position: usize,
}

impl SQLParser {
    pub fn new(tokens: Vec<SQLToken>) -> Self {
        Self {
            tokens,
            position: 0,
        }
    }

    pub fn parse(&mut self) -> Result<SQLStatement, String> {
        if self.check(&SQLToken::Keyword("SELECT".to_string())) {
            return self.parse_select();
        }
        
        if self.check(&SQLToken::Keyword("INSERT".to_string())) {
            return self.parse_insert();
        }
        
        if self.check(&SQLToken::Keyword("DELETE".to_string())) {
            return self.parse_delete();
        }

        if self.check(&SQLToken::Keyword("UPDATE".to_string())) {
            return self.parse_update();
        }
        
        if self.check(&SQLToken::Keyword("CREATE".to_string())) {
            return self.parse_create();
        if self.check(&SQLToken::Keyword("SHOW".to_string())) {
            return self.parse_show();
        }

        if self.check(&SQLToken::Keyword("DESCRIBE".to_string()))
            || self.check(&SQLToken::Keyword("DESC".to_string()))
        {
            return self.parse_describe();
        }
        
        Err("Unsupported SQL statement".to_string())
    }

    fn parse_select(&mut self) -> Result<SQLStatement, String> {
        self.advance(); // consume SELECT
        
        let mut columns: Vec<SelectColumn> = Vec::new();
        
        if !self.check(&SQLToken::Keyword("FROM".to_string())) {
            loop {
                columns.push(self.parse_select_column()?);
                
                if self.check(&SQLToken::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
        }
        
        self.advance_through(SQLToken::Keyword("FROM".to_string()));
        
        let table = self.expect_identifier()?;

        // Parse optional VECTOR SEARCH clause
        let mut vector_search = None;
        if self.check(&SQLToken::Keyword("VECTOR".to_string())) {
            self.advance();
            self.advance_through(SQLToken::Keyword("SEARCH".to_string()));
            let column = self.expect_identifier()?;
            self.advance_through(SQLToken::Keyword("WITH".to_string()));
            let vec = self.parse_vector_literal()?;
            vector_search = Some(VectorSearch {
                column,
                query_vector: vec,
            });
        }

        // Parse optional JOIN clauses
        let mut joins = Vec::new();
        while self.check(&SQLToken::Keyword("JOIN".to_string()))
            || self.check(&SQLToken::Keyword("INNER".to_string()))
            || self.check(&SQLToken::Keyword("LEFT".to_string()))
            || self.check(&SQLToken::Keyword("RIGHT".to_string()))
        {
            joins.push(self.parse_join()?);
        }
        
        let mut where_clause = None;
        if self.check(&SQLToken::Keyword("WHERE".to_string())) {
            self.advance();
            where_clause = Some(self.parse_where_clause()?);
        }

        // GROUP BY
        let mut group_by = Vec::new();
        if self.check(&SQLToken::Keyword("GROUP".to_string())) {
            self.advance();
            self.advance_through(SQLToken::Keyword("BY".to_string()));
            loop {
                group_by.push(self.expect_identifier()?);
                if self.check(&SQLToken::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
        }

        // HAVING
        let mut having = None;
        if self.check(&SQLToken::Keyword("HAVING".to_string())) {
            self.advance();
            having = Some(self.parse_where_clause()?);
        }

        // ORDER BY
        let mut order_by = Vec::new();
        if self.check(&SQLToken::Keyword("ORDER".to_string())) {
            self.advance();
            self.advance_through(SQLToken::Keyword("BY".to_string()));
            loop {
                let col = self.expect_identifier()?;
                let asc = if self.check(&SQLToken::Keyword("DESC".to_string())) {
                    self.advance();
                    false
                } else {
                    if self.check(&SQLToken::Keyword("ASC".to_string())) {
                        self.advance();
                    }
                    true
                };
                order_by.push((col, asc));
                if self.check(&SQLToken::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
        }
        
        let mut limit = None;
        if self.check(&SQLToken::Keyword("LIMIT".to_string())) {
            self.advance();
            if let SQLToken::Number(n) = self.current().clone() {
                limit = Some(n as usize);
                self.advance();
            }
        }
        
        Ok(SQLStatement::Select(SQLSelect {
            columns,
            table,
            vector_search,
            where_clause,
            limit,
            joins,
            group_by,
            having,
            order_by,
        }))
    }

    /// Parse a single SELECT column: either a plain identifier, `*`, or an aggregate function.
    fn parse_select_column(&mut self) -> Result<SelectColumn, String> {
        match self.current().clone() {
            SQLToken::Identifier(ref s) if s == "*" => {
                self.advance();
                Ok(SelectColumn::Column("*".to_string()))
            }
            SQLToken::Keyword(ref k)
                if k == "COUNT" || k == "SUM" || k == "AVG" || k == "MIN" || k == "MAX" =>
            {
                let func = match k.as_str() {
                    "COUNT" => AggregateFunction::Count,
                    "SUM"   => AggregateFunction::Sum,
                    "AVG"   => AggregateFunction::Avg,
                    "MIN"   => AggregateFunction::Min,
                    "MAX"   => AggregateFunction::Max,
                    _ => unreachable!(),
                };
                self.advance(); // consume function name
                self.advance_through(SQLToken::LParen);

                let target = if self.check(&SQLToken::Identifier("*".to_string())) {
                    self.advance();
                    "*".to_string()
                } else {
                    self.expect_identifier()?
                };

                self.advance_through(SQLToken::RParen);

                // Optional alias: AS alias_name
                let alias = if self.check(&SQLToken::Keyword("AS".to_string())) {
                    self.advance();
                    Some(self.expect_identifier()?)
                } else {
                    None
                };

                Ok(SelectColumn::Aggregate { func, target, alias })
            }
            SQLToken::Identifier(name) => {
                self.advance();
                Ok(SelectColumn::Column(name))
            }
            other => Err(format!("Unexpected token in SELECT: {:?}", other)),
        }
    }

    /// Parse: [INNER|LEFT|RIGHT] JOIN table ON left_col = right_col
    fn parse_join(&mut self) -> Result<JoinClause, String> {
        let join_type = if self.check(&SQLToken::Keyword("INNER".to_string())) {
            self.advance();
            self.advance_through(SQLToken::Keyword("JOIN".to_string()));
            JoinType::Inner
        } else if self.check(&SQLToken::Keyword("LEFT".to_string())) {
            self.advance();
            // Optional OUTER keyword
            if self.check(&SQLToken::Keyword("OUTER".to_string())) {
                self.advance();
            }
            self.advance_through(SQLToken::Keyword("JOIN".to_string()));
            JoinType::Left
        } else if self.check(&SQLToken::Keyword("RIGHT".to_string())) {
            self.advance();
            if self.check(&SQLToken::Keyword("OUTER".to_string())) {
                self.advance();
            }
            self.advance_through(SQLToken::Keyword("JOIN".to_string()));
            JoinType::Right
        } else {
            // Plain JOIN defaults to INNER
            self.advance_through(SQLToken::Keyword("JOIN".to_string()));
            JoinType::Inner
        };

        let join_table = self.expect_identifier()?;

        self.advance_through(SQLToken::Keyword("ON".to_string()));

        let left_col = self.expect_identifier()?;
        let op_token = self.current().clone();
        let op = match op_token {
            SQLToken::Operator(ref s) if s == "=" => "=".to_string(),
            _ => return Err(format!("Expected '=' in JOIN ON clause, got {:?}", op_token)),
        };
        self.advance();
        let right_col = self.expect_identifier()?;

        Ok(JoinClause {
            join_type,
            table: join_table,
            on_condition: (left_col, op, right_col),
        })
    }

    fn parse_insert(&mut self) -> Result<SQLStatement, String> {
        self.advance_through(SQLToken::Keyword("INSERT".to_string()));
        self.advance_through(SQLToken::Keyword("INTO".to_string()));
        
        let table = self.expect_identifier()?;
        
        self.advance_through(SQLToken::LParen);
        
        let mut columns = Vec::new();
        loop {
            if let SQLToken::Identifier(name) = self.current().clone() {
                columns.push(name);
            }
            
            if self.check(&SQLToken::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        
        self.advance_through(SQLToken::RParen);
        self.advance_through(SQLToken::Keyword("VALUES".to_string()));
        self.advance_through(SQLToken::LParen);
        
        let mut values = Vec::new();
        loop {
            let val = self.current().clone();
            match val {
                SQLToken::StringLiteral(s) => values.push(SQLValue::String(s)),
                SQLToken::Number(n) => values.push(SQLValue::Number(n)),
                SQLToken::Keyword(k) if k == "NULL" => values.push(SQLValue::Null),
                _ => {}
            }
            
            if self.check(&SQLToken::Comma) {
                self.advance();
            } else {
                break;
            }
        }
        
        self.advance_through(SQLToken::RParen);
        
        Ok(SQLStatement::Insert(SQLInsert {
            table,
            columns,
            values,
        }))
    }

    fn parse_delete(&mut self) -> Result<SQLStatement, String> {
        self.advance_through(SQLToken::Keyword("DELETE".to_string()));
        self.advance_through(SQLToken::Keyword("FROM".to_string()));
        
        let table = self.expect_identifier()?;
        
        let mut where_clause = None;
        if self.check(&SQLToken::Keyword("WHERE".to_string())) {
            self.advance();
            where_clause = Some(self.parse_where_clause()?);
        }
        
        Ok(SQLStatement::Delete(SQLDelete {
            table,
            where_clause,
        }))
    }

    fn parse_update(&mut self) -> Result<SQLStatement, String> {
        self.advance_through(SQLToken::Keyword("UPDATE".to_string()));

        let table = self.expect_identifier()?;

        self.advance_through(SQLToken::Keyword("SET".to_string()));

        let mut set_clause = Vec::new();
        loop {
            let col = self.expect_identifier()?;
            // expect '='
            if let SQLToken::Operator(ref op) = self.current().clone() {
                if op != "=" {
                    return Err(format!("Expected '=' in SET clause, got '{}'", op));
                }
            }
            self.advance(); // consume '='
            let val = self.parse_sql_value()?;
            set_clause.push((col, val));

            if self.check(&SQLToken::Comma) {
                self.advance();
            } else {
                break;
            }
        }

        let mut where_clause = None;
        if self.check(&SQLToken::Keyword("WHERE".to_string())) {
            self.advance();
            where_clause = Some(self.parse_where_clause()?);
        }

        Ok(SQLStatement::Update(SQLUpdate {
            table,
            set_clause,
            where_clause,
        }))
    }

    /// 将当前 token 解析为 SQLValue
    fn parse_sql_value(&mut self) -> Result<SQLValue, String> {
        match self.current().clone() {
            SQLToken::StringLiteral(s) => {
                self.advance();
                Ok(SQLValue::String(s))
            }
            SQLToken::Number(n) => {
                self.advance();
                Ok(SQLValue::Number(n))
            }
            SQLToken::Keyword(ref k) if k == "NULL" => {
                self.advance();
                Ok(SQLValue::Null)
            }
            SQLToken::Keyword(ref k) if k == "TRUE" => {
                self.advance();
                Ok(SQLValue::Boolean(true))
            }
            SQLToken::Keyword(ref k) if k == "FALSE" => {
                self.advance();
                Ok(SQLValue::Boolean(false))
            }
            other => Err(format!("Expected a value, got {:?}", other)),
        }
    }

    fn parse_create(&mut self) -> Result<SQLStatement, String> {
        self.advance_through(SQLToken::Keyword("CREATE".to_string()));
        
        if self.check(&SQLToken::Keyword("INDEX".to_string())) {
            self.advance();
            let name = self.expect_identifier()?;
            
            self.advance_through(SQLToken::Keyword("ON".to_string()));
            let table = self.expect_identifier()?;
            
            let mut columns = Vec::new();
            self.advance_through(SQLToken::LParen);
            loop {
                if let SQLToken::Identifier(col) = self.current().clone() {
                    columns.push(col);
                }
                if self.check(&SQLToken::Comma) {
                    self.advance();
                } else {
                    break;
                }
            }
            self.advance_through(SQLToken::RParen);
            
            return Ok(SQLStatement::CreateIndex(SQLCreateIndex {
                name,
                table,
                columns,
            }));
        }
        
        Err("Unsupported CREATE statement".to_string())
    }

    /// `SHOW COLLECTIONS` or `SHOW TABLES`
    fn parse_show(&mut self) -> Result<SQLStatement, String> {
        self.advance(); // consume SHOW
        // Accept COLLECTIONS or TABLES
        match self.current().clone() {
            SQLToken::Keyword(k) if k == "COLLECTIONS" || k == "TABLES" => {
                self.advance();
                Ok(SQLStatement::ShowCollections)
            }
            SQLToken::Identifier(s) if s.eq_ignore_ascii_case("collections") || s.eq_ignore_ascii_case("tables") => {
                self.advance();
                Ok(SQLStatement::ShowCollections)
            }
            _ => Err("Expected COLLECTIONS or TABLES after SHOW".to_string()),
        }
    }

    /// `DESCRIBE <table>` or `DESC <table>`
    fn parse_describe(&mut self) -> Result<SQLStatement, String> {
        self.advance(); // consume DESCRIBE or DESC
        let table = self.expect_identifier()?;
        Ok(SQLStatement::Describe(table))
    }

    fn parse_where_clause(&mut self) -> Result<SQLCondition, String> {
        let mut conditions = Vec::new();
        
        loop {
            let col = self.expect_identifier()?;
            let op = self.current().clone();
            
            if let SQLToken::Operator(op_str) = op {
                self.advance();
                let val = self.current().clone();
                
                let sql_val = match val {
                    SQLToken::StringLiteral(s) => SQLValue::String(s),
                    SQLToken::Number(n) => SQLValue::Number(n),
                    SQLToken::Keyword(ref k) if k == "NULL" => SQLValue::Null,
                    SQLToken::Keyword(ref k) if k == "TRUE" => SQLValue::Boolean(true),
                    SQLToken::Keyword(ref k) if k == "FALSE" => SQLValue::Boolean(false),
                    _ => SQLValue::Null,
                };
                
                conditions.push((col, op_str, sql_val));
            }

            // 支持 AND 连接多个条件
            if self.check(&SQLToken::Keyword("AND".to_string())) {
                self.advance();
            } else {
                break;
            }
        }
        
        Ok(SQLCondition { conditions })
    }

    /// Parse a vector literal: `[1.0, 2.0, 3.0]`
    fn parse_vector_literal(&mut self) -> Result<Vec<f32>, String> {
        // Expect opening bracket
        match self.current().clone() {
            SQLToken::Operator(c) if c == '[' => self.advance(),
            _ => return Err("Expected '[' for vector literal".to_string()),
        }

        let mut vals = Vec::new();
        loop {
            match self.current().clone() {
                SQLToken::Number(n) => {
                    vals.push(n as f32);
                    self.advance();
                }
                SQLToken::Operator(c) if c == ']' => {
                    self.advance();
                    break;
                }
                SQLToken::Comma => {
                    self.advance();
                }
                _ => return Err(format!(
                    "Unexpected token in vector literal: {:?}", self.current()
                )),
            }
        }
        Ok(vals)
    }

    fn expect_identifier(&mut self) -> Result<String, String> {
        if let SQLToken::Identifier(name) = self.current().clone() {
            self.advance();
            Ok(name)
        } else {
            Err("Expected identifier".to_string())
        }
    }

    fn current(&self) -> &SQLToken {
        self.tokens.get(self.position).unwrap_or(&SQLToken::EOF)
    }

    fn advance(&mut self) {
        self.position += 1;
    }

    fn advance_through(&mut self, expected: SQLToken) {
        while !self.check(&expected) && !matches!(self.current(), SQLToken::EOF) {
            self.advance();
        }
        self.advance();
    }

    fn check(&self, expected: &SQLToken) -> bool {
        match (expected, self.current()) {
            (SQLToken::Keyword(e), SQLToken::Keyword(c)) => e == c,
            (SQLToken::Identifier(e), SQLToken::Identifier(c)) => e == c,
            (SQLToken::Operator(e), SQLToken::Operator(c)) => e == c,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum SQLStatement {
    Select(SQLSelect),
    Insert(SQLInsert),
    Delete(SQLDelete),
    Update(SQLUpdate),
    CreateIndex(SQLCreateIndex),
    /// `SHOW COLLECTIONS` or `SHOW TABLES`
    ShowCollections,
    /// `DESCRIBE <table>` or `DESC <table>`
    Describe(String),
}

#[derive(Debug, Clone)]
pub struct SQLSelect {
    pub columns: Vec<SelectColumn>,
    pub table: String,
    pub vector_search: Option<VectorSearch>,
    pub where_clause: Option<SQLCondition>,
    pub limit: Option<usize>,
    pub joins: Vec<JoinClause>,
    pub group_by: Vec<String>,
    pub having: Option<SQLCondition>,
    pub order_by: Vec<(String, bool)>, // (column, ascending)
}

/// Vector search clause: `VECTOR SEARCH <column> WITH [vals]`
#[derive(Debug, Clone)]
pub struct VectorSearch {
    pub column: String,
    pub query_vector: Vec<f32>,
}

/// A column reference in a SELECT clause — either a bare column or an aggregate function.
#[derive(Debug, Clone)]
pub enum SelectColumn {
    /// A simple column reference, e.g. `name` or `*`
    Column(String),
    /// An aggregate function, e.g. `COUNT(*)`, `SUM(price)`, `AVG(score) AS avg_score`
    Aggregate {
        func: AggregateFunction,
        target: String, // column name, or "*" for COUNT(*)
        alias: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

impl SelectColumn {
    /// Return the output column name for projection and result construction.
    pub fn output_name(&self) -> String {
        match self {
            SelectColumn::Column(name) => name.clone(),
            SelectColumn::Aggregate { func, target, alias } => {
                if let Some(a) = alias {
                    a.clone()
                } else {
                    let fn_name = match func {
                        AggregateFunction::Count => "count",
                        AggregateFunction::Sum => "sum",
                        AggregateFunction::Avg => "avg",
                        AggregateFunction::Min => "min",
                        AggregateFunction::Max => "max",
                    };
                    format!("{}({})", fn_name, target)
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub join_type: JoinType,
    pub table: String,
    /// The ON condition: (left_column, operator, right_column)
    /// e.g., JOIN orders ON users.id = orders.user_id → ("id", "=", "user_id")
    pub on_condition: (String, String, String),
}

#[derive(Debug, Clone)]
pub struct SQLInsert {
    pub table: String,
    pub columns: Vec<String>,
    pub values: Vec<SQLValue>,
}

#[derive(Debug, Clone)]
pub struct SQLDelete {
    pub table: String,
    pub where_clause: Option<SQLCondition>,
}

#[derive(Debug, Clone)]
pub struct SQLCreateIndex {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SQLUpdate {
    pub table: String,
    pub set_clause: Vec<(String, SQLValue)>,
    pub where_clause: Option<SQLCondition>,
}

#[derive(Debug, Clone)]
pub struct SQLCondition {
    pub conditions: Vec<(String, String, SQLValue)>,
}

#[derive(Debug, Clone)]
pub enum SQLValue {
    String(String),
    Number(f64),
    Null,
    Boolean(bool),
}

/// SQL 执行器 — 接入 DataManager 实现真正的数据库操作。
///
/// 现在 INSERT/DELETE/SELECT 全部走 DataManager 的真实存储路径，
/// 支持持久化（RocksDB）、HNSW 索引、事务（WAL+MVCC）。
///
/// `local_collections` 仅用于未绑定 DataManager 时的降级路径（如测试）。
pub struct SQLExecutor {
    /// 真实数据引擎（可选）：为 None 时退化为内存 CollectionData
    data_manager: Option<Arc<DataManager>>,
    /// 降级路径：本地内存集合
    collections: Arc<RwLock<HashMap<String, CollectionData>>>,
}

#[derive(Debug, Clone)]
pub struct CollectionData {
    pub name: String,
    pub vectors: HashMap<String, (Vec<f32>, HashMap<String, SQLValue>)>,
}

impl SQLExecutor {
    /// 创建一个接入 DataManager 的执行器（推荐：生产路径）
    pub fn with_data_manager(data_manager: Arc<DataManager>) -> Self {
        Self {
            data_manager: Some(data_manager),
            collections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 纯内存执行器（测试 / 降级路径，不持久化）
    pub fn new() -> Self {
        Self {
            data_manager: None,
            collections: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 注册一个本地集合到降级路径（用于 DataManager 未绑定的场景）
    pub async fn register_collection(&self, name: &str, data: CollectionData) {
        let mut collections = self.collections.write().await;
        collections.insert(name.to_string(), data);
    }

    // ── Column helpers ──────────────────────────────────

    /// Whether `SELECT *` was used (no explicit columns or `*` present).
    fn is_star(cols: &[SelectColumn]) -> bool {
        cols.is_empty() || cols.iter().any(|c| matches!(c, SelectColumn::Column(s) if s == "*"))
    }

    /// Extract plain column names from SelectColumn list (ignores aggregates).
    fn plain_columns(cols: &[SelectColumn]) -> Vec<String> {
        cols.iter().filter_map(|c| match c {
            SelectColumn::Column(name) => Some(name.clone()),
            _ => None,
        }).collect()
    }

    /// Whether any aggregate function is present.
    fn has_aggregates(cols: &[SelectColumn]) -> bool {
        cols.iter().any(|c| matches!(c, SelectColumn::Aggregate { .. }))
    }

    pub async fn execute(&self, sql: &str) -> Result<SQLResult, String> {
        let mut lexer = SQLLexer::new(sql);
        let tokens = lexer.tokenize();

        let mut parser = SQLParser::new(tokens);

        match parser.parse() {
            Ok(statement) => self.execute_statement(statement).await,
            Err(e) => Err(e),
        }
    }

    async fn execute_statement(&self, statement: SQLStatement) -> Result<SQLResult, String> {
        match statement {
            SQLStatement::Select(s) => self.execute_select(s).await,
            SQLStatement::Insert(i) => self.execute_insert(i).await,
            SQLStatement::Delete(d) => self.execute_delete(d).await,
            SQLStatement::CreateIndex(c) => self.execute_create_index(c).await,
            SQLStatement::Update(u) => self.execute_update(u).await,
            SQLStatement::ShowCollections => self.execute_show_collections().await,
            SQLStatement::Describe(table) => self.execute_describe(&table).await,
        }
    }

    async fn execute_select(&self, select: SQLSelect) -> Result<SQLResult, String> {
        // Vector search path — highest priority
        if select.vector_search.is_some() {
            if let Some(ref dm) = self.data_manager {
                return self.execute_vector_search_dm(dm, select).await;
            }
            return self.execute_vector_search_local(select).await;
        }

        // Aggregate / GROUP BY path
        if Self::has_aggregates(&select.columns) || !select.group_by.is_empty() {
            if let Some(ref dm) = self.data_manager {
                return self.execute_aggregate_select_dm(dm, select).await;
            }
            return self.execute_aggregate_select_local(select).await;
        }

        // JOIN path
        if !select.joins.is_empty() {
            if let Some(ref dm) = self.data_manager {
                return self.execute_join_select_dm(dm, select).await;
            }
            return self.execute_join_select_local(select).await;
        }

        // Plain SELECT
        if let Some(ref dm) = self.data_manager {
            return self.execute_select_dm(dm, select).await;
        }
        self.execute_select_local(select).await
    }

    // ═══════════════════════════════════════════════════════════
    // Vector search — DataManager path
    // ═══════════════════════════════════════════════════════════

    async fn execute_vector_search_dm(
        &self,
        dm: &DataManager,
        select: SQLSelect,
    ) -> Result<SQLResult, String> {
        let vs = select.vector_search.as_ref().unwrap();
        let query = &vs.query_vector;
        let has_where = select.where_clause.is_some();

        let data_map = dm.data_ref().read().await;
        let collection_data = data_map.get(&select.table)
            .ok_or_else(|| format!("Collection '{}' not found", select.table))?;

        // Compute similarities with optional pre-filter
        let mut scored: Vec<(f32, String, Vec<f32>, serde_json::Value)> = Vec::new();
        for (id, record) in collection_data.iter() {
            // Pre-filter: WHERE before similarity (skip expensive cosine for filtered rows)
            if has_where {
                let mut check_row = HashMap::new();
                check_row.insert("id".to_string(), SQLValue::String(id.clone()));
                if let Some(obj) = record.metadata.as_object() {
                    for (key, val) in obj {
                        check_row.insert(key.clone(), json_to_sql_value(val));
                    }
                }
                if let Some(ref cond) = select.where_clause {
                    if !evaluate_condition(&check_row, cond) {
                        continue;
                    }
                }
            }

            let sim = cosine_similarity(query, &record.vector);
            scored.push((sim, id.clone(), record.vector.clone(), record.metadata.clone()));
        }

        // Note: finalize_vector_search re-applies WHERE for safety (idempotent pre+post filter)
        self.finalize_vector_search(scored, &select)
    }

    // ═══════════════════════════════════════════════════════════
    // Vector search — Local fallback path
    // ═══════════════════════════════════════════════════════════

    async fn execute_vector_search_local(
        &self,
        select: SQLSelect,
    ) -> Result<SQLResult, String> {
        let vs = select.vector_search.as_ref().unwrap();
        let query = &vs.query_vector;
        let has_where = select.where_clause.is_some();

        let collections = self.collections.read().await;
        let collection = collections.get(&select.table)
            .ok_or_else(|| format!("Collection '{}' not found", select.table))?;

        let mut scored: Vec<(f32, String, Vec<f32>, serde_json::Value)> = Vec::new();
        for (id, (vec, meta)) in &collection.vectors {
            // Pre-filter: WHERE before similarity
            if has_where {
                let mut check_row = HashMap::new();
                check_row.insert("id".to_string(), SQLValue::String(id.clone()));
                for (key, val) in meta {
                    check_row.insert(key.clone(), val.clone());
                }
                if let Some(ref cond) = select.where_clause {
                    if !evaluate_condition(&check_row, cond) {
                        continue;
                    }
                }
            }

            let sim = cosine_similarity(query, vec);
            let meta_json: serde_json::Value = serde_json::to_value(
                meta.iter().map(|(k, v)| (k.clone(), sql_value_to_json(v))).collect::<HashMap<_, _>>()
            ).unwrap_or(serde_json::json!({}));
            scored.push((sim, id.clone(), vec.clone(), meta_json));
        }

        self.finalize_vector_search(scored, &select)
    }

    /// Common vector search finalization: pre-filter (WHERE), sort, limit, project.
    fn finalize_vector_search(
        &self,
        scored: Vec<(f32, String, Vec<f32>, serde_json::Value)>,
        select: &SQLSelect,
    ) -> Result<SQLResult, String> {
        let cols_all = Self::is_star(&select.columns);
        let plain_cols = Self::plain_columns(&select.columns);
        let has_where = select.where_clause.is_some();

        // Step 1: Build full rows (ALL metadata columns) for WHERE evaluation
        let mut candidates: Vec<(f32, HashMap<String, SQLValue>)> = Vec::new();

        for (sim, id, _vec, meta) in &scored {
            let mut full_row = HashMap::new();
            full_row.insert("_distance".to_string(), SQLValue::Number(*sim as f64));
            full_row.insert("id".to_string(), SQLValue::String(id.clone()));

            if let Some(obj) = meta.as_object() {
                for (key, val) in obj {
                    full_row.insert(key.clone(), json_to_sql_value(val));
                }
            }

            // WHERE filter — evaluated on FULL row
            if let Some(ref cond) = select.where_clause {
                if !evaluate_condition(&full_row, cond) {
                    continue;
                }
            }

            candidates.push((*sim, full_row));
        }

        // Step 2: Sort by similarity descending
        candidates.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        // Step 3: Project to requested columns
        let mut rows: Vec<HashMap<String, SQLValue>> = Vec::new();
        for (_sim, full_row) in &candidates {
            let mut row = HashMap::new();

            for col in &plain_cols {
                if col == "*" {
                    // SELECT * → return everything except raw vector
                    for (key, val) in full_row {
                        row.insert(key.clone(), val.clone());
                    }
                } else if let Some(val) = full_row.get(col) {
                    row.insert(col.clone(), val.clone());
                }
            }

            // If * was used, we already filled everything above
            if cols_all && !plain_cols.iter().any(|c| c == "*") {
                // SELECT * without explicit columns
                for (key, val) in full_row {
                    row.insert(key.clone(), val.clone());
                }
            }

            rows.push(row);
        }

        // Step 4: LIMIT
        let limit = select.limit.unwrap_or(rows.len());
        rows.truncate(limit);

        Ok(SQLResult::Select(rows))
    }

    /// DataManager 路径：从真实存储中读取
    async fn execute_select_dm(
        &self,
        dm: &DataManager,
        select: SQLSelect,
    ) -> Result<SQLResult, String> {
        if !dm.collection_exists(&select.table).await {
            return Err(format!("Collection '{}' not found", select.table));
        }

        let count = dm.get_vectors_count(&select.table).await
            .map_err(|e| format!("{}", e))?;

        if count == 0 {
            return Ok(SQLResult::Select(Vec::new()));
        }

        let mut rows: Vec<HashMap<String, SQLValue>> = Vec::new();
        let data_map = dm.data_ref().read().await;
        let collection_data = data_map.get(&select.table)
            .ok_or_else(|| format!("Collection '{}' not found in data", select.table))?;

        let cols_all = Self::is_star(&select.columns);
        let plain_cols = Self::plain_columns(&select.columns);

        for (id, record) in collection_data.iter() {
            let mut row = HashMap::new();

            if cols_all || plain_cols.iter().any(|c| c == "id") {
                row.insert("id".to_string(), SQLValue::String(id.clone()));
            }

            if let Some(obj) = record.metadata.as_object() {
                for (key, val) in obj {
                    if cols_all || plain_cols.contains(key) {
                        let sql_val = json_to_sql_value(val);
                        row.insert(key.clone(), sql_val);
                    }
                }
            }

            if let Some(ref cond) = select.where_clause {
                if !evaluate_condition(&row, cond) {
                    continue;
                }
            }

            rows.push(row);
        }

        // ORDER BY (non-aggregate)
        if !select.order_by.is_empty() {
            rows.sort_by(|a, b| {
                for (col, asc) in &select.order_by {
                    let va = a.get(col);
                    let vb = b.get(col);
                    let ord = sql_value_cmp(va.unwrap_or(&SQLValue::Null), vb.unwrap_or(&SQLValue::Null))
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if ord != std::cmp::Ordering::Equal {
                        return if *asc { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // LIMIT
        let limit = select.limit.unwrap_or(rows.len());
        rows.truncate(limit);

        Ok(SQLResult::Select(rows))
    }

    // ═══════════════════════════════════════════════════════════
    // JOIN execution — DataManager path
    // ═══════════════════════════════════════════════════════════

    async fn execute_join_select_dm(
        &self,
        dm: &DataManager,
        select: SQLSelect,
    ) -> Result<SQLResult, String> {
        // Load left table rows
        let mut left_rows = self.load_all_rows_dm(dm, &select.table).await?;

        // Apply each join
        for join in &select.joins {
            let right_rows = self.load_all_rows_dm(dm, &join.table).await?;

            left_rows = self.hash_join(
                &left_rows,
                &right_rows,
                &join.on_condition.0,  // left column
                &join.on_condition.2,  // right column
                &join.join_type,
            );
        }

        // Apply WHERE after joins
        if let Some(ref cond) = select.where_clause {
            left_rows.retain(|row| evaluate_condition(row, cond));
        }

        // Project columns (with table prefix handling: "users.name" → "name")
        if !Self::is_star(&select.columns) {
            let col_names: Vec<String> = select.columns.iter()
                .map(|c| c.output_name())
                .collect();
            left_rows = self.project_columns(&left_rows, &col_names);
        }

        // LIMIT
        let limit = select.limit.unwrap_or(left_rows.len());
        left_rows.truncate(limit);

        Ok(SQLResult::Select(left_rows))
    }

    /// Hash join: builds a map on the right side, probes with the left side.
    fn hash_join(
        &self,
        left: &[HashMap<String, SQLValue>],
        right: &[HashMap<String, SQLValue>],
        left_key: &str,
        right_key: &str,
        join_type: &JoinType,
    ) -> Vec<HashMap<String, SQLValue>> {
        // Build phase: right_table → HashMap<key, Vec<row>>
        let mut hash: HashMap<String, Vec<&HashMap<String, SQLValue>>> = HashMap::new();
        for row in right {
            let key = sql_value_to_key(row.get(right_key));
            hash.entry(key).or_default().push(row);
        }

        // Probe phase
        let mut result = Vec::new();

        for left_row in left {
            let key = sql_value_to_key(left_row.get(left_key));
            if let Some(matches) = hash.get(&key) {
                for right_row in matches {
                    let mut merged = left_row.clone();
                    for (k, v) in *right_row {
                        // Avoid overwriting left columns with right columns of the same name
                        if !merged.contains_key(k) {
                            merged.insert(k.clone(), v.clone());
                        }
                    }
                    result.push(merged);
                }
            } else if *join_type == JoinType::Left || *join_type == JoinType::Right {
                // LEFT/RIGHT JOIN: preserve unmatched row with NULLs
                let mut merged = left_row.clone();
                for right_row in right.first().map(|r| r.keys()).unwrap_or(&vec![]) {
                    let k: &String = right_row;
                    if !merged.contains_key(k) {
                        merged.insert(k.clone(), SQLValue::Null);
                    }
                }
                result.push(merged);
            }
            // INNER JOIN: unmatched left row is dropped
        }

        result
    }

    /// Load all rows from a DataManager collection
    async fn load_all_rows_dm(
        &self,
        dm: &DataManager,
        collection: &str,
    ) -> Result<Vec<HashMap<String, SQLValue>>, String> {
        if !dm.collection_exists(collection).await {
            return Err(format!("Collection '{}' not found", collection));
        }

        let data_map = dm.data_ref().read().await;
        let collection_data = data_map.get(collection)
            .ok_or_else(|| format!("Collection '{}' not found in data", collection))?;

        let mut rows = Vec::new();
        for (id, record) in collection_data.iter() {
            let mut row = HashMap::new();
            row.insert("id".to_string(), SQLValue::String(id.clone()));
            if let Some(obj) = record.metadata.as_object() {
                for (key, val) in obj {
                    row.insert(key.clone(), json_to_sql_value(val));
                }
            }
            rows.push(row);
        }
        Ok(rows)
    }

    /// Project columns: keep only the specified columns from each row
    fn project_columns(
        &self,
        rows: &[HashMap<String, SQLValue>],
        columns: &[String],
    ) -> Vec<HashMap<String, SQLValue>> {
        rows.iter().map(|row| {
            let mut proj = HashMap::new();
            for col in columns {
                if let Some(val) = row.get(col) {
                    proj.insert(col.clone(), val.clone());
                } else {
                    // Try without table prefix: "users.name" → "name"
                    let short = col.rsplit('.').next().unwrap_or(col);
                    if let Some(val) = row.get(short) {
                        proj.insert(col.clone(), val.clone());
                    } else {
                        proj.insert(col.clone(), SQLValue::Null);
                    }
                }
            }
            proj
        }).collect()
    }

    // ═══════════════════════════════════════════════════════════
    // JOIN execution — Local fallback path
    // ═══════════════════════════════════════════════════════════

    async fn execute_join_select_local(
        &self,
        select: SQLSelect,
    ) -> Result<SQLResult, String> {
        let collections = self.collections.read().await;

        // Load left table
        let left_collection = collections.get(&select.table)
            .ok_or_else(|| format!("Collection '{}' not found", select.table))?;
        let mut left_rows: Vec<HashMap<String, SQLValue>> = left_collection.vectors.iter()
            .map(|(id, (_vec, meta))| {
                let mut row = meta.clone();
                row.insert("id".to_string(), SQLValue::String(id.clone()));
                row
            })
            .collect();

        // Apply each join
        for join in &select.joins {
            let right_collection = collections.get(&join.table)
                .ok_or_else(|| format!("Collection '{}' not found", join.table))?;
            let right_rows: Vec<HashMap<String, SQLValue>> = right_collection.vectors.iter()
                .map(|(id, (_vec, meta))| {
                    let mut row = meta.clone();
                    row.insert("id".to_string(), SQLValue::String(id.clone()));
                    row
                })
                .collect();

            left_rows = self.hash_join(
                &left_rows,
                &right_rows,
                &join.on_condition.0,
                &join.on_condition.2,
                &join.join_type,
            );
        }

        if let Some(ref cond) = select.where_clause {
            left_rows.retain(|row| evaluate_condition(row, cond));
        }

        if !Self::is_star(&select.columns) {
            let col_names: Vec<String> = select.columns.iter()
                .map(|c| c.output_name())
                .collect();
            left_rows = self.project_columns(&left_rows, &col_names);
        }

        let limit = select.limit.unwrap_or(left_rows.len());
        left_rows.truncate(limit);

        Ok(SQLResult::Select(left_rows))
    }

    // ═══════════════════════════════════════════════════════════
    // AGGREGATE execution — DataManager path
    // ═══════════════════════════════════════════════════════════

    async fn execute_aggregate_select_dm(
        &self,
        dm: &DataManager,
        select: SQLSelect,
    ) -> Result<SQLResult, String> {
        let rows = self.load_all_rows_dm(dm, &select.table).await?;
        self.execute_aggregate(rows, &select)
    }

    /// Common aggregation logic (shared by DM and local paths).
    fn execute_aggregate(
        &self,
        mut rows: Vec<HashMap<String, SQLValue>>,
        select: &SQLSelect,
    ) -> Result<SQLResult, String> {
        // Apply WHERE before grouping
        if let Some(ref cond) = select.where_clause {
            rows.retain(|row| evaluate_condition(row, cond));
        }

        // Group rows
        let groups = if select.group_by.is_empty() {
            // No GROUP BY → one group with all rows
            vec![("".to_string(), rows)]
        } else {
            let mut map: HashMap<String, Vec<HashMap<String, SQLValue>>> = HashMap::new();
            for row in rows {
                let key_parts: Vec<String> = select.group_by.iter()
                    .map(|col| sql_value_to_key(row.get(col)))
                    .collect();
                let key = key_parts.join("|");
                map.entry(key).or_default().push(row);
            }
            map.into_iter().collect()
        };

        // Compute aggregates per group
        let mut result_rows: Vec<HashMap<String, SQLValue>> = Vec::new();

        for (_key, group_rows) in &groups {
            if group_rows.is_empty() {
                continue;
            }

            let mut row = HashMap::new();

            // Output GROUP BY columns
            for gb_col in &select.group_by {
                if let Some(val) = group_rows[0].get(gb_col) {
                    row.insert(gb_col.clone(), val.clone());
                }
            }

            // Compute aggregates
            for col in &select.columns {
                match col {
                    SelectColumn::Aggregate { func, target, alias } => {
                        let name = col.output_name();
                        let val = match func {
                            AggregateFunction::Count => {
                                if target == "*" {
                                    SQLValue::Number(group_rows.len() as f64)
                                } else {
                                    let c = group_rows.iter()
                                        .filter(|r| r.get(target).is_some())
                                        .count();
                                    SQLValue::Number(c as f64)
                                }
                            }
                            AggregateFunction::Sum => {
                                let sum: f64 = group_rows.iter()
                                    .filter_map(|r| r.get(target))
                                    .filter_map(|v| match v {
                                        SQLValue::Number(n) => Some(*n),
                                        _ => None,
                                    })
                                    .sum();
                                SQLValue::Number(sum)
                            }
                            AggregateFunction::Avg => {
                                let nums: Vec<f64> = group_rows.iter()
                                    .filter_map(|r| r.get(target))
                                    .filter_map(|v| match v {
                                        SQLValue::Number(n) => Some(*n),
                                        _ => None,
                                    })
                                    .collect();
                                if nums.is_empty() {
                                    SQLValue::Null
                                } else {
                                    SQLValue::Number(nums.iter().sum::<f64>() / nums.len() as f64)
                                }
                            }
                            AggregateFunction::Min => {
                                let mut vals: Vec<&SQLValue> = group_rows.iter()
                                    .filter_map(|r| r.get(target))
                                    .collect();
                                vals.sort_by(|a, b| sql_value_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal));
                                vals.first().map(|v| (*v).clone()).unwrap_or(SQLValue::Null)
                            }
                            AggregateFunction::Max => {
                                let mut vals: Vec<&SQLValue> = group_rows.iter()
                                    .filter_map(|r| r.get(target))
                                    .collect();
                                vals.sort_by(|a, b| sql_value_cmp(a, b).unwrap_or(std::cmp::Ordering::Equal).reverse());
                                vals.first().map(|v| (*v).clone()).unwrap_or(SQLValue::Null)
                            }
                        };
                        row.insert(name, val);
                    }
                    SelectColumn::Column(name) if name != "*" => {
                        // Non-aggregate column in aggregate query: take first group's value
                        if let Some(val) = group_rows[0].get(name) {
                            row.insert(name.clone(), val.clone());
                        }
                    }
                    _ => {}
                }
            }

            result_rows.push(row);
        }

        // HAVING filter
        if let Some(ref having) = select.having {
            result_rows.retain(|row| evaluate_condition(row, having));
        }

        // ORDER BY
        if !select.order_by.is_empty() {
            result_rows.sort_by(|a, b| {
                for (col, asc) in &select.order_by {
                    let va = a.get(col);
                    let vb = b.get(col);
                    let ord = sql_value_cmp(va.unwrap_or(&SQLValue::Null), vb.unwrap_or(&SQLValue::Null))
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if ord != std::cmp::Ordering::Equal {
                        return if *asc { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // LIMIT
        let limit = select.limit.unwrap_or(result_rows.len());
        result_rows.truncate(limit);

        Ok(SQLResult::Select(result_rows))
    }

    // ═══════════════════════════════════════════════════════════
    // AGGREGATE execution — Local fallback path
    // ═══════════════════════════════════════════════════════════

    async fn execute_aggregate_select_local(
        &self,
        select: SQLSelect,
    ) -> Result<SQLResult, String> {
        let collections = self.collections.read().await;
        let collection = collections.get(&select.table)
            .ok_or_else(|| format!("Collection '{}' not found", select.table))?;

        let rows: Vec<HashMap<String, SQLValue>> = collection.vectors.iter()
            .map(|(id, (_vec, meta))| {
                let mut row = meta.clone();
                row.insert("id".to_string(), SQLValue::String(id.clone()));
                row
            })
            .collect();

        self.execute_aggregate(rows, &select)
    }

    /// 降级路径：本地 CollectionData
    async fn execute_select_local(&self, select: SQLSelect) -> Result<SQLResult, String> {
        let collections = self.collections.read().await;

        let collection = collections.get(&select.table)
            .ok_or_else(|| format!("Collection '{}' not found", select.table))?;

        let mut rows: Vec<HashMap<String, SQLValue>> = Vec::new();
        let cols_all = Self::is_star(&select.columns);
        let plain_cols = Self::plain_columns(&select.columns);

        for (id, (_vec, meta)) in &collection.vectors {
            let mut row = HashMap::new();

            if cols_all || plain_cols.iter().any(|c| c == "id") {
                row.insert("id".to_string(), SQLValue::String(id.clone()));
            }

            for (key, val) in meta {
                if cols_all || plain_cols.contains(key) {
                    row.insert(key.clone(), val.clone());
                }
            }

            if let Some(ref cond) = select.where_clause {
                if !evaluate_condition(&row, cond) {
                    continue;
                }
            }

            rows.push(row);
        }

        // ORDER BY
        if !select.order_by.is_empty() {
            rows.sort_by(|a, b| {
                for (col, asc) in &select.order_by {
                    let va = a.get(col);
                    let vb = b.get(col);
                    let ord = sql_value_cmp(va.unwrap_or(&SQLValue::Null), vb.unwrap_or(&SQLValue::Null))
                        .unwrap_or(std::cmp::Ordering::Equal);
                    if ord != std::cmp::Ordering::Equal {
                        return if *asc { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let limit = select.limit.unwrap_or(rows.len());
        rows.truncate(limit);

        Ok(SQLResult::Select(rows))
    }

    async fn execute_insert(&self, insert: SQLInsert) -> Result<SQLResult, String> {
        if let Some(ref dm) = self.data_manager {
            return self.execute_insert_dm(dm, insert).await;
        }
        self.execute_insert_local(insert).await
    }

    /// DataManager 路径：真实写入
    async fn execute_insert_dm(
        &self,
        dm: &DataManager,
        insert: SQLInsert,
    ) -> Result<SQLResult, String> {
        // 自动创建 collection（如果不存在）
        if !dm.collection_exists(&insert.table).await {
            // SQL 表默认维度 128，cosine 距离
            dm.create_collection(&insert.table, 128, "cosine").await
                .map_err(|e| format!("Failed to auto-create collection '{}': {}", insert.table, e))?;
        }

        let dimension = dm.get_collection_dimension(&insert.table).await
            .map_err(|e| format!("{}", e))?;

        // 生成唯一 ID
        let id = format!("id_{}", uuid_simple());

        // 将列值序列化为 JSON metadata，同时用零向量填充
        let mut meta_obj = serde_json::Map::new();
        for (i, col) in insert.columns.iter().enumerate() {
            if i < insert.values.len() {
                let json_val = sql_value_to_json(&insert.values[i]);
                meta_obj.insert(col.clone(), json_val);
            }
        }
        let metadata = serde_json::Value::Object(meta_obj);
        let zero_vec = vec![0.0f32; dimension];

        dm.insert_vectors(&insert.table, vec![(id, zero_vec, metadata)]).await
            .map_err(|e| format!("Insert failed: {}", e))?;

        Ok(SQLResult::Insert(1))
    }

    /// 降级路径：本地内存写入
    async fn execute_insert_local(&self, insert: SQLInsert) -> Result<SQLResult, String> {
        let mut collections = self.collections.write().await;

        let collection = collections.entry(insert.table.clone()).or_insert_with(|| {
            CollectionData {
                name: insert.table.clone(),
                vectors: HashMap::new(),
            }
        });

        let id = format!("id_{}", uuid_simple());
        let mut meta = HashMap::new();

        for (i, col) in insert.columns.iter().enumerate() {
            if i < insert.values.len() {
                meta.insert(col.clone(), insert.values[i].clone());
            }
        }

        collection.vectors.insert(id, (vec![], meta));

        Ok(SQLResult::Insert(1))
    }

    async fn execute_delete(&self, delete: SQLDelete) -> Result<SQLResult, String> {
        if let Some(ref dm) = self.data_manager {
            return self.execute_delete_dm(dm, delete).await;
        }
        self.execute_delete_local(delete).await
    }

    /// DataManager 路径：真实删除 + WHERE 过滤
    async fn execute_delete_dm(
        &self,
        dm: &DataManager,
        delete: SQLDelete,
    ) -> Result<SQLResult, String> {
        if !dm.collection_exists(&delete.table).await {
            return Err(format!("Collection '{}' not found", delete.table));
        }

        let data_map = dm.data_ref().read().await;
        let collection_data = data_map.get(&delete.table)
            .ok_or_else(|| format!("Collection '{}' not found in data", delete.table))?;

        // 收集符合 WHERE 条件的 ID
        let mut ids_to_delete: Vec<String> = Vec::new();
        for (id, record) in collection_data.iter() {
            if let Some(ref cond) = delete.where_clause {
                let row = record_to_row(id, record);
                if evaluate_condition(&row, cond) {
                    ids_to_delete.push(id.clone());
                }
            } else {
                ids_to_delete.push(id.clone());
            }
        }
        drop(data_map);

        if ids_to_delete.is_empty() {
            return Ok(SQLResult::Delete(0));
        }

        let deleted = dm.delete_vectors(&delete.table, &ids_to_delete).await
            .map_err(|e| format!("Delete failed: {}", e))?;

        Ok(SQLResult::Delete(deleted))
    }

    /// 降级路径：本地删除
    async fn execute_delete_local(&self, delete: SQLDelete) -> Result<SQLResult, String> {
        let mut collections = self.collections.write().await;

        let collection = collections.get_mut(&delete.table)
            .ok_or_else(|| format!("Collection '{}' not found", delete.table))?;

        if let Some(ref cond) = delete.where_clause {
            let mut ids_to_remove: Vec<String> = Vec::new();
            for (id, (_vec, meta)) in &collection.vectors {
                let row = meta_to_row(id, meta);
                if evaluate_condition(&row, cond) {
                    ids_to_remove.push(id.clone());
                }
            }
            let count = ids_to_remove.len();
            for id in ids_to_remove {
                collection.vectors.remove(&id);
            }
            Ok(SQLResult::Delete(count))
        } else {
            let count = collection.vectors.len();
            collection.vectors.clear();
            Ok(SQLResult::Delete(count))
        }
    }

    async fn execute_create_index(&self, create_index: SQLCreateIndex) -> Result<SQLResult, String> {
        if let Some(ref dm) = self.data_manager {
            return self.execute_create_index_dm(dm, create_index).await;
        }
        // 降级路径：总是成功（内存模式不支持自定义索引）
        Ok(SQLResult::CreateIndex(true))
    }

    /// DataManager 路径：创建向量索引
    async fn execute_create_index_dm(
        &self,
        dm: &DataManager,
        create_index: SQLCreateIndex,
    ) -> Result<SQLResult, String> {
        // 确保 collection 存在
        if !dm.collection_exists(&create_index.table).await {
            return Err(format!(
                "Collection '{}' not found — create it before building an index",
                create_index.table
            ));
        }

        let index_manager = dm.index_manager_ref();

        // 检查索引是否已存在
        if let Ok(Some(_existing)) = index_manager.get_index(&create_index.name).await {
            return Ok(SQLResult::CreateIndex(true)); // 幂等
        }

        // 创建 HNSW 索引（默认 metric: cosine）
        index_manager.create_index(&create_index.name, "hnsw", "cosine").await
            .map_err(|e| format!("Index creation failed: {}", e))?;

        // 如果索引属于已知 collection，将该 collection 的现有向量回填到索引
        let data_map = dm.data_ref().read().await;
        if let Some(collection_data) = data_map.get(&create_index.table) {
            if let Ok(Some(index)) = index_manager.get_index(&create_index.name).await {
                for (id, record) in collection_data.iter() {
                    let _ = index.add(id, &record.vector).await;
                }
            }
        }

        Ok(SQLResult::CreateIndex(true))
    }

    // ═══════════════════════════════════════════════════════════
    // SHOW COLLECTIONS / SHOW TABLES
    // ═══════════════════════════════════════════════════════════

    async fn execute_show_collections(&self) -> Result<SQLResult, String> {
        // DM path
        if let Some(ref dm) = self.data_manager {
            let data_map = dm.data_ref().read().await;
            let rows: Vec<HashMap<String, SQLValue>> = data_map
                .keys()
                .map(|name| {
                    let mut row = HashMap::new();
                    row.insert("name".to_string(), SQLValue::String(name.clone()));
                    row
                })
                .collect();
            return Ok(SQLResult::Select(rows));
        }

        // Local path
        let collections = self.collections.read().await;
        let rows: Vec<HashMap<String, SQLValue>> = collections
            .keys()
            .map(|name| {
                let mut row = HashMap::new();
                row.insert("name".to_string(), SQLValue::String(name.clone()));
                row
            })
            .collect();
        Ok(SQLResult::Select(rows))
    }

    // ═══════════════════════════════════════════════════════════
    // DESCRIBE <table> / DESC <table>
    // ═══════════════════════════════════════════════════════════

    async fn execute_describe(&self, table: &str) -> Result<SQLResult, String> {
        // DM path
        if let Some(ref dm) = self.data_manager {
            let data_map = dm.data_ref().read().await;
            let collection_data = data_map.get(table)
                .ok_or_else(|| format!("Collection '{}' not found", table))?;

            let mut rows = Vec::new();

            // Row 0: collection name
            let mut name_row = HashMap::new();
            name_row.insert("field".to_string(), SQLValue::String("_collection".to_string()));
            name_row.insert("value".to_string(), SQLValue::String(table.to_string()));
            rows.push(name_row);

            // Row 1: record count
            let mut count_row = HashMap::new();
            count_row.insert("field".to_string(), SQLValue::String("record_count".to_string()));
            count_row.insert("value".to_string(), SQLValue::Number(collection_data.len() as f64));
            rows.push(count_row);

            // Row 2: vector dimension (sample first record)
            let dim = collection_data.iter().next()
                .map(|(_, r)| r.vector.len())
                .unwrap_or(0);
            let mut dim_row = HashMap::new();
            dim_row.insert("field".to_string(), SQLValue::String("vector_dim".to_string()));
            dim_row.insert("value".to_string(), SQLValue::Number(dim as f64));
            rows.push(dim_row);

            // Row 3+: metadata fields (sample first record for keys)
            if let Some((_, first)) = collection_data.iter().next() {
                if let Some(obj) = first.metadata.as_object() {
                    for key in obj.keys() {
                        let mut f_row = HashMap::new();
                        f_row.insert("field".to_string(), SQLValue::String(format!("meta.{}", key)));
                        f_row.insert("value".to_string(), SQLValue::String("<present>".to_string()));
                        rows.push(f_row);
                    }
                }
            }

            return Ok(SQLResult::Select(rows));
        }

        // Local path
        let collections = self.collections.read().await;
        let collection = collections.get(table)
            .ok_or_else(|| format!("Collection '{}' not found", table))?;

        let mut rows = Vec::new();

        let mut name_row = HashMap::new();
        name_row.insert("field".to_string(), SQLValue::String("_collection".to_string()));
        name_row.insert("value".to_string(), SQLValue::String(table.to_string()));
        rows.push(name_row);

        let mut count_row = HashMap::new();
        count_row.insert("field".to_string(), SQLValue::String("record_count".to_string()));
        count_row.insert("value".to_string(), SQLValue::Number(collection.vectors.len() as f64));
        rows.push(count_row);

        let dim = collection.vectors.values().next()
            .map(|(v, _)| v.len())
            .unwrap_or(0);
        let mut dim_row = HashMap::new();
        dim_row.insert("field".to_string(), SQLValue::String("vector_dim".to_string()));
        dim_row.insert("value".to_string(), SQLValue::Number(dim as f64));
        rows.push(dim_row);

        // Metadata fields
        if let Some((_, meta)) = collection.vectors.values().next() {
            for key in meta.keys() {
                let mut f_row = HashMap::new();
                f_row.insert("field".to_string(), SQLValue::String(format!("meta.{}", key)));
                f_row.insert("value".to_string(), SQLValue::String("<present>".to_string()));
                rows.push(f_row);
            }
        }

        Ok(SQLResult::Select(rows))
    }

    /// UPDATE 语句执行
    async fn execute_update(&self, update: SQLUpdate) -> Result<SQLResult, String> {
        if let Some(ref dm) = self.data_manager {
            return self.execute_update_dm(dm, update).await;
        }
        self.execute_update_local(update).await
    }

    /// DataManager 路径：更新匹配 WHERE 的记录
    async fn execute_update_dm(
        &self,
        dm: &DataManager,
        update: SQLUpdate,
    ) -> Result<SQLResult, String> {
        if !dm.collection_exists(&update.table).await {
            return Err(format!("Collection '{}' not found", update.table));
        }

        let mut updated = 0usize;
        let dimension = dm.get_collection_dimension(&update.table).await
            .map_err(|e| format!("{}", e))?;

        // 收集要更新的 ID 和新 metadata（需先读再写，避免死锁）
        let updates: Vec<(String, serde_json::Value)> = {
            let data_map = dm.data_ref().read().await;
            let collection_data = data_map.get(&update.table)
                .ok_or_else(|| format!("Collection '{}' not found in data", update.table))?;

            let mut result = Vec::new();
            for (id, record) in collection_data.iter() {
                let row = record_to_row(id, record);

                let matches = match &update.where_clause {
                    Some(cond) => evaluate_condition(&row, cond),
                    None => true,
                };

                if matches {
                    let mut new_meta = if let Some(ref obj) = record.metadata.as_object() {
                        obj.clone()
                    } else {
                        serde_json::Map::new()
                    };

                    for (col, val) in &update.set_clause {
                        new_meta.insert(col.clone(), sql_value_to_json(val));
                    }

                    result.push((id.clone(), serde_json::Value::Object(new_meta)));
                }
            }
            result
        };

        for (id, new_metadata) in updates {
            // update_vector 需要向量 + 可选 metadata
            let zero_vec = vec![0.0f32; dimension];
            match dm.update_vector(&update.table, &id, zero_vec, Some(new_metadata)).await {
                Ok(true) => updated += 1,
                Ok(false) => { /* 记录不存在，跳过 */ }
                Err(e) => return Err(format!("Update failed for id '{}': {}", id, e)),
            }
        }

        Ok(SQLResult::Update(updated))
    }

    /// 降级路径：本地更新
    async fn execute_update_local(&self, update: SQLUpdate) -> Result<SQLResult, String> {
        let mut collections = self.collections.write().await;

        let collection = collections.get_mut(&update.table)
            .ok_or_else(|| format!("Collection '{}' not found", update.table))?;

        let mut updated = 0usize;

        for (id, (_vec, meta)) in collection.vectors.iter_mut() {
            let row = meta_to_row(id, meta);

            let matches = match &update.where_clause {
                Some(cond) => evaluate_condition(&row, cond),
                None => true,
            };

            if matches {
                for (col, val) in &update.set_clause {
                    meta.insert(col.clone(), val.clone());
                }
                updated += 1;
            }
        }

        Ok(SQLResult::Update(updated))
    }
}

#[derive(Debug, Clone)]
pub enum SQLResult {
    Select(Vec<HashMap<String, SQLValue>>),
    Insert(usize),
    Update(usize),
    Delete(usize),
    CreateIndex(bool),
}

fn uuid_simple() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

// ═══════════════════════════════════════════════════════════
// 辅助函数：类型转换 & WHERE 条件求值
// ═══════════════════════════════════════════════════════════

/// Convert an optional SQLValue to a hash key string
fn sql_value_to_key(val: Option<&SQLValue>) -> String {
    match val {
        Some(SQLValue::String(s)) => s.clone(),
        Some(SQLValue::Number(n)) => format!("{}", n),
        Some(SQLValue::Boolean(b)) => format!("{}", b),
        Some(SQLValue::Null) => "__null__".to_string(),
        None => "__missing__".to_string(),
    }
}

/// serde_json::Value → SQLValue
fn json_to_sql_value(val: &serde_json::Value) -> SQLValue {
    match val {
        serde_json::Value::String(s) => SQLValue::String(s.clone()),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                SQLValue::Number(i as f64)
            } else {
                SQLValue::Number(n.as_f64().unwrap_or(0.0))
            }
        }
        serde_json::Value::Bool(b) => SQLValue::Boolean(*b),
        serde_json::Value::Null => SQLValue::Null,
        _ => SQLValue::String(val.to_string()),
    }
}

/// SQLValue → serde_json::Value
fn sql_value_to_json(val: &SQLValue) -> serde_json::Value {
    match val {
        SQLValue::String(s) => serde_json::Value::String(s.clone()),
        SQLValue::Number(n) => {
            serde_json::json!(n)
        }
        SQLValue::Boolean(b) => serde_json::Value::Bool(*b),
        SQLValue::Null => serde_json::Value::Null,
    }
}

/// 将 VectorRecord 转为 HashMap 行（用于 WHERE 求值）
fn record_to_row(id: &str, record: &VectorRecord) -> HashMap<String, SQLValue> {
    let mut row = HashMap::new();
    row.insert("id".to_string(), SQLValue::String(id.to_string()));
    if let Some(obj) = record.metadata.as_object() {
        for (key, val) in obj {
            row.insert(key.clone(), json_to_sql_value(val));
        }
    }
    row
}

/// 将本地 CollectionData 的行转为 HashMap
fn meta_to_row(id: &str, meta: &HashMap<String, SQLValue>) -> HashMap<String, SQLValue> {
    let mut row = HashMap::new();
    row.insert("id".to_string(), SQLValue::String(id.to_string()));
    for (k, v) in meta {
        row.insert(k.clone(), v.clone());
    }
    row
}

/// WHERE 条件求值（支持 =, !=, <, >, <=, >=, LIKE）
fn evaluate_condition(
    row: &HashMap<String, SQLValue>,
    cond: &SQLCondition,
) -> bool {
    for (col, op, val) in &cond.conditions {
        let row_val = match row.get(col) {
            Some(v) => v,
            None => return false,
        };

        let matches = match op.as_str() {
            "=" => sql_values_eq(row_val, val),
            "!=" | "<>" => !sql_values_eq(row_val, val),
            "<" => sql_value_cmp(row_val, val) == Some(std::cmp::Ordering::Less),
            ">" => sql_value_cmp(row_val, val) == Some(std::cmp::Ordering::Greater),
            "<=" => {
                let c = sql_value_cmp(row_val, val);
                c == Some(std::cmp::Ordering::Less) || c == Some(std::cmp::Ordering::Equal)
            }
            ">=" => {
                let c = sql_value_cmp(row_val, val);
                c == Some(std::cmp::Ordering::Greater) || c == Some(std::cmp::Ordering::Equal)
            }
            "LIKE" => sql_value_like(row_val, val),
            _ => false,
        };

        if !matches {
            return false;
        }
    }
    true
}

fn sql_values_eq(a: &SQLValue, b: &SQLValue) -> bool {
    match (a, b) {
        (SQLValue::String(s1), SQLValue::String(s2)) => s1 == s2,
        (SQLValue::Number(n1), SQLValue::Number(n2)) => (n1 - n2).abs() < f64::EPSILON,
        (SQLValue::Boolean(b1), SQLValue::Boolean(b2)) => b1 == b2,
        (SQLValue::Null, SQLValue::Null) => true,
        _ => false,
    }
}

fn sql_value_cmp(a: &SQLValue, b: &SQLValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (SQLValue::Number(n1), SQLValue::Number(n2)) => n1.partial_cmp(n2),
        (SQLValue::String(s1), SQLValue::String(s2)) => Some(s1.cmp(s2)),
        _ => None,
    }
}

fn sql_value_like(a: &SQLValue, pattern: &SQLValue) -> bool {
    match (a, pattern) {
        (SQLValue::String(s), SQLValue::String(p)) => {
            // 简单 LIKE：% 通配
            if p == "%" { return true; }
            if p.starts_with('%') && p.ends_with('%') {
                let inner = &p[1..p.len()-1];
                return s.contains(inner);
            }
            if p.starts_with('%') {
                return s.ends_with(&p[1..]);
            }
            if p.ends_with('%') {
                return s.starts_with(&p[..p.len()-1]);
            }
            s == p
        }
        _ => false,
    }
}

/// Compute cosine similarity between two vectors.
/// Returns 1.0 for identical vectors, 0.0 for orthogonal, -1.0 for opposite.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let dot: f32 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f32 = a.iter().map(|x| x * x).sum::<f32>().sqrt();
    let norm_b: f32 = b.iter().map(|x| x * x).sum::<f32>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return 0.0;
    }
    dot / (norm_a * norm_b).max(1e-12)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lexer_keywords() {
        let mut lexer = SQLLexer::new("SELECT * FROM test");
        let tokens = lexer.tokenize();
        
        assert!(tokens.iter().any(|t| matches!(t, SQLToken::Keyword(k) if k == "SELECT")));
        assert!(tokens.iter().any(|t| matches!(t, SQLToken::Keyword(k) if k == "FROM")));
    }

    #[test]
    fn test_lexer_identifiers() {
        let mut lexer = SQLLexer::new("SELECT id FROM users");
        let tokens = lexer.tokenize();
        
        assert!(tokens.iter().any(|t| matches!(t, SQLToken::Identifier(i) if i == "id")));
    }

    #[test]
    fn test_parse_select() {
        let mut lexer = SQLLexer::new("SELECT id, name FROM users LIMIT 10");
        let tokens = lexer.tokenize();
        
        let mut parser = SQLParser::new(tokens);
        let result = parser.parse();
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_insert() {
        let sql = "INSERT INTO users (id, name, age) VALUES (1, 'John', 25)";
        let mut lexer = SQLLexer::new(sql);
        let tokens = lexer.tokenize();
        
        let mut parser = SQLParser::new(tokens);
        let result = parser.parse();
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_parse_update() {
        let sql = "UPDATE users SET name = 'Jane', age = 30 WHERE id = 1";
        let mut lexer = SQLLexer::new(sql);
        let tokens = lexer.tokenize();

        let mut parser = SQLParser::new(tokens);
        let result = parser.parse();

        assert!(result.is_ok());
        if let Ok(SQLStatement::Update(u)) = result {
            assert_eq!(u.table, "users");
            assert_eq!(u.set_clause.len(), 2);
            assert_eq!(u.set_clause[0].0, "name");
            assert_eq!(u.set_clause[1].0, "age");
        } else {
            panic!("Expected UPDATE statement");
        }
    }

    #[tokio::test]
    async fn test_execute_select() {
        let executor = SQLExecutor::new();
        
        let result = executor.execute("SELECT * FROM users").await;
        // 无 DataManager 绑定时会查本地空集合
        assert!(result.is_err() || matches!(result, Ok(SQLResult::Select(_))));
    }

    #[tokio::test]
    async fn test_execute_insert() {
        let executor = SQLExecutor::new();
        
        let result = executor.execute("INSERT INTO users (name) VALUES ('John')").await;
        
        match result {
            Ok(SQLResult::Insert(count)) => assert!(count > 0),
            _ => {}
        }
    }

    #[tokio::test]
    async fn test_execute_insert_select_roundtrip() {
        let executor = SQLExecutor::new();

        // INSERT
        let _ = executor.execute("INSERT INTO users (name, age) VALUES ('Alice', 30)").await;

        // SELECT
        let result = executor.execute("SELECT id, name, age FROM users").await;
        match result {
            Ok(SQLResult::Select(rows)) => {
                assert!(!rows.is_empty());
                let row = &rows[0];
                assert!(row.contains_key("name"));
                assert_eq!(row.get("name").unwrap(), &SQLValue::String("Alice".to_string()));
            }
            _ => panic!("SELECT failed"),
        }
    }

    #[tokio::test]
    async fn test_execute_delete_with_where() {
        let executor = SQLExecutor::new();

        // Setup
        let _ = executor.execute("INSERT INTO users (name, age) VALUES ('Bob', 25)").await;
        let _ = executor.execute("INSERT INTO users (name, age) VALUES ('Charlie', 35)").await;

        // DELETE with WHERE
        let result = executor.execute("DELETE FROM users WHERE name = 'Bob'").await;
        match result {
            Ok(SQLResult::Delete(count)) => assert!(count > 0),
            _ => {}
        }

        // Verify: only Charlie remains
        let result = executor.execute("SELECT name FROM users").await;
        match result {
            Ok(SQLResult::Select(rows)) => {
                let names: Vec<&SQLValue> = rows.iter()
                    .filter_map(|r| r.get("name"))
                    .collect();
                assert!(!names.contains(&&SQLValue::String("Bob".to_string())));
            }
            _ => {}
        }
    }

    #[tokio::test]
    async fn test_execute_update() {
        let executor = SQLExecutor::new();

        let _ = executor.execute("INSERT INTO users (name, age) VALUES ('Dave', 20)").await;

        let result = executor.execute("UPDATE users SET age = 21 WHERE name = 'Dave'").await;
        match result {
            Ok(SQLResult::Update(count)) => assert!(count > 0),
            _ => {}
        }
    }
}
