//! Formatting utilities for Arrow [`RecordBatch`] query results.

use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;

/// Renders a slice of [`RecordBatch`]es as an ASCII table.
///
/// Returns `"(0 rows)"` when there is no data to display.
#[must_use]
pub fn format_batches(batches: &[RecordBatch]) -> String {
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return "(0 rows)".to_string();
    }
    match pretty_format_batches(batches) {
        Ok(table) => table.to_string(),
        Err(e) => format!("Error formatting results: {e}"),
    }
}

/// Returns the total number of rows across all batches.
#[must_use]
pub fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn make_batch(ids: &[i32], names: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids.to_vec())),
                Arc::new(StringArray::from(
                    names.iter().copied().collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_format_batches_empty_vec() {
        assert_eq!(format_batches(&[]), "(0 rows)");
    }

    #[test]
    fn test_format_batches_zero_row_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(Vec::<i32>::new()))])
                .unwrap();
        assert_eq!(format_batches(&[batch]), "(0 rows)");
    }

    #[test]
    fn test_format_batches_with_data() {
        let batch = make_batch(&[1, 2], &["alice", "bob"]);
        let output = format_batches(&[batch]);
        assert!(output.contains("id"));
        assert!(output.contains("name"));
        assert!(output.contains("alice"));
        assert!(output.contains("bob"));
    }

    #[test]
    fn test_format_batches_multiple_batches() {
        let b1 = make_batch(&[1], &["a"]);
        let b2 = make_batch(&[2], &["b"]);
        let output = format_batches(&[b1, b2]);
        assert!(output.contains("a"));
        assert!(output.contains("b"));
    }

    #[test]
    fn test_row_count_empty() {
        assert_eq!(row_count(&[]), 0);
    }

    #[test]
    fn test_row_count_single_batch() {
        let batch = make_batch(&[1, 2, 3], &["a", "b", "c"]);
        assert_eq!(row_count(&[batch]), 3);
    }

    #[test]
    fn test_row_count_multiple_batches() {
        let b1 = make_batch(&[1, 2], &["a", "b"]);
        let b2 = make_batch(&[3], &["c"]);
        assert_eq!(row_count(&[b1, b2]), 3);
    }
}
