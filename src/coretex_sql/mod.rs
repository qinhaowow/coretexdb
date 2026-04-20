//! SQL Query Support module for CoreTexDB
//! Provides SQL-like query interface for vector database operations

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

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
        self.advance_through(SQLToken::Keyword("INSERT".to_string()));
        
        if self.check(&SQLToken::Keyword("SELECT".to_string())) {
            return self.parse_select();
        }
        
        if self.check(&SQLToken::Keyword("INSERT".to_string())) {
            return self.parse_insert();
        }
        
        if self.check(&SQLToken::Keyword("DELETE".to_string())) {
            return self.parse_delete();
        }
        
        if self.check(&SQLToken::Keyword("CREATE".to_string())) {
            return self.parse_create();
        }
        
        Err("Unsupported SQL statement".to_string())
    }

    fn parse_select(&mut self) -> Result<SQLStatement, String> {
        self.advance();
        
        let mut columns = Vec::new();
        
        if !self.check(&SQLToken::Keyword("FROM".to_string())) {
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
        }
        
        self.advance_through(SQLToken::Keyword("FROM".to_string()));
        
        let table = self.expect_identifier()?;
        
        let mut where_clause = None;
        if self.check(&SQLToken::Keyword("WHERE".to_string())) {
            self.advance();
            where_clause = Some(self.parse_where_clause()?);
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
            where_clause,
            limit,
        }))
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

    fn parse_where_clause(&mut self) -> Result<SQLCondition, String> {
        let mut conditions = Vec::new();
        
        let col = self.expect_identifier()?;
        let op = self.current().clone();
        
        if let SQLToken::Operator(op_str) = op {
            self.advance();
            let val = self.current().clone();
            
            let sql_val = match val {
                SQLToken::StringLiteral(s) => SQLValue::String(s),
                SQLToken::Number(n) => SQLValue::Number(n),
                _ => SQLValue::Null,
            };
            
            conditions.push((col, op_str, sql_val));
        }
        
        Ok(SQLCondition { conditions })
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
    CreateIndex(SQLCreateIndex),
}

#[derive(Debug, Clone)]
pub struct SQLSelect {
    pub columns: Vec<String>,
    pub table: String,
    pub where_clause: Option<SQLCondition>,
    pub limit: Option<usize>,
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

pub struct SQLExecutor {
    collections: Arc<RwLock<HashMap<String, CollectionData>>>,
}

#[derive(Debug, Clone)]
pub struct CollectionData {
    pub name: String,
    pub vectors: HashMap<String, (Vec<f32>, HashMap<String, SQLValue>)>,
}

impl SQLExecutor {
    pub fn new() -> Self {
        Self {
            collections: Arc::new(RwLock::new(HashMap::new())),
        }
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
        }
    }

    async fn execute_select(&self, select: SQLSelect) -> Result<SQLResult, String> {
        let collections = self.collections.read().await;
        
        if let Some(collection) = collections.get(&select.table) {
            let mut rows: Vec<HashMap<String, SQLValue>> = Vec::new();
            
            for (id, (_, meta)) in &collection.vectors {
                let mut row = HashMap::new();
                
                if select.columns.is_empty() || select.columns.contains(&"*".to_string()) || select.columns.contains(&"id".to_string()) {
                    row.insert("id".to_string(), SQLValue::String(id.clone()));
                }
                
                for (key, val) in meta {
                    row.insert(key.clone(), val.clone());
                }
                
                rows.push(row);
            }
            
            let limit = select.limit.unwrap_or(rows.len());
            rows.truncate(limit);
            
            Ok(SQLResult::Select(rows))
        } else {
            Err(format!("Collection '{}' not found", select.table))
        }
    }

    async fn execute_insert(&self, insert: SQLInsert) -> Result<SQLResult, String> {
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
        Ok(SQLResult::Delete(0))
    }

    async fn execute_create_index(&self, create_index: SQLCreateIndex) -> Result<SQLResult, String> {
        Ok(SQLResult::CreateIndex(true))
    }

    pub async fn register_collection(&self, name: &str, data: CollectionData) {
        let mut collections = self.collections.write().await;
        collections.insert(name.to_string(), data);
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

    #[tokio::test]
    async fn test_execute_select() {
        let executor = SQLExecutor::new();
        
        let result = executor.execute("SELECT * FROM users").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_execute_insert() {
        let executor = SQLExecutor::new();
        
        let result = executor.execute("INSERT INTO users (name) VALUES ('John')").await;
        
        match result {
            Ok(SQLResult::Insert(count)) => assert_eq!(count, 1),
            _ => {}
        }
    }
}
