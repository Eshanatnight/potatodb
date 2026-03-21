use chrono::{DateTime, NaiveDateTime};

/// Rewrites PostgreSQL-style `EXPLAIN (FORMAT JSON)` into `DataFusion`'s
/// accepted form by stripping unsupported option lists.
pub fn normalize_explain_sql(sql: &str) -> String {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();
    if upper.starts_with("EXPLAIN (") {
        let open_idx = trimmed.find('(');
        let close_idx = trimmed.find(')');
        if let (Some(open), Some(close)) = (open_idx, close_idx)
            && close > open
        {
            let opts = upper[open + 1..close].trim();
            let has_analyze = opts.split(',').any(|o| o.trim() == "ANALYZE");
            let rest = trimmed[close + 1..].trim();
            if !rest.is_empty() {
                return if has_analyze {
                    format!("EXPLAIN ANALYZE {rest}")
                } else {
                    format!("EXPLAIN {rest}")
                };
            }
        }
    }
    trimmed.to_string()
}

pub fn is_read_only_sql(sql: &str) -> bool {
    let first = sql.split_whitespace().next().unwrap_or("").to_uppercase();
    matches!(
        first.as_str(),
        "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "EXPLAIN"
    )
}

/// Strips `AS OF TIMESTAMP '...'` from a SQL statement.
pub fn strip_as_of_timestamp(sql: &str) -> String {
    let upper = sql.to_uppercase();
    let Some(idx) = upper.find(" AS OF TIMESTAMP ") else {
        return sql.to_string();
    };
    let after = &sql[idx + " AS OF TIMESTAMP ".len()..];
    let mut skip = 0usize;
    if let Some(stripped) = after.strip_prefix('\'') {
        if let Some(end_quote) = stripped.find('\'') {
            skip = 1 + end_quote + 1;
        }
    } else {
        skip = after.find(' ').unwrap_or(after.len());
    }
    let mut rewritten = String::new();
    rewritten.push_str(&sql[..idx]);
    rewritten.push_str(after.get(skip..).unwrap_or_default());
    rewritten
}

pub fn parse_as_of_timestamp(sql: &str) -> Option<i64> {
    let upper = sql.to_uppercase();
    let idx = upper.find(" AS OF TIMESTAMP ")?;
    let after = sql[idx + " AS OF TIMESTAMP ".len()..].trim_start();
    let token = if let Some(stripped) = after.strip_prefix('\'') {
        let end = stripped.find('\'')?;
        stripped[..end].to_string()
    } else {
        after
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .trim_end_matches(';')
            .to_string()
    };
    if token.is_empty() {
        return None;
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(&token) {
        return Some(dt.timestamp_millis());
    }
    if let Ok(naive) = NaiveDateTime::parse_from_str(&token, "%Y-%m-%d %H:%M:%S") {
        return Some(naive.and_utc().timestamp_millis());
    }
    token.parse::<i64>().ok()
}

#[cfg(test)]
mod tests {
    use super::{
        is_read_only_sql, normalize_explain_sql, parse_as_of_timestamp, strip_as_of_timestamp,
    };

    #[test]
    fn normalize_explain_rewrites_option_list() {
        let sql = "EXPLAIN (FORMAT JSON, ANALYZE) SELECT * FROM t";
        assert_eq!(
            normalize_explain_sql(sql),
            "EXPLAIN ANALYZE SELECT * FROM t"
        );
    }

    #[test]
    fn normalize_explain_without_analyze_drops_options() {
        let sql = "EXPLAIN (FORMAT JSON) SELECT * FROM t";
        assert_eq!(normalize_explain_sql(sql), "EXPLAIN SELECT * FROM t");
    }

    #[test]
    fn readonly_sql_detection_is_strict() {
        assert!(is_read_only_sql("select 1"));
        assert!(is_read_only_sql("WITH x AS (SELECT 1) SELECT * FROM x"));
        assert!(!is_read_only_sql("INSERT INTO t VALUES (1)"));
    }

    #[test]
    fn strip_as_of_timestamp_removes_clause() {
        let sql = "SELECT * FROM events AS OF TIMESTAMP '2026-03-19T10:11:12Z';";
        assert_eq!(strip_as_of_timestamp(sql), "SELECT * FROM events;");
    }

    #[test]
    fn parse_as_of_timestamp_supports_numeric() {
        let sql = "SELECT * FROM events AS OF TIMESTAMP 1710840300000;";
        assert_eq!(parse_as_of_timestamp(sql), Some(1_710_840_300_000));
    }

    #[test]
    fn parse_as_of_timestamp_supports_rfc3339() {
        let sql = "SELECT * FROM events AS OF TIMESTAMP '2026-03-19T10:11:12Z';";
        assert!(parse_as_of_timestamp(sql).is_some());
    }
}
