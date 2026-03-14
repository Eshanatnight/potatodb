//! Formatting utilities for Arrow [`RecordBatch`] query results.

use arrow::array::Array;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use arrow_cast::display::{ArrayFormatter, FormatOptions};

/// Maximum rows to display before truncating with head/tail preview.
const MAX_DISPLAY_ROWS: usize = 40;

/// Renders a slice of [`RecordBatch`]es as a plain ASCII table (Arrow format).
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

/// Formats record batches as a box-drawing table with column types and
/// automatic truncation for large result sets.
///
/// Uses Unicode box-drawing characters (`┌─┬┐│├┼┤└┴┘`). When the total row
/// count exceeds [`MAX_DISPLAY_ROWS`], shows first/last halves separated by
/// `·` rows, with a merged summary footer.
#[must_use]
pub fn format_batches_truncated(batches: &[RecordBatch]) -> String {
    if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
        return "(0 rows)".to_string();
    }

    let total = row_count(batches);
    let schema = batches[0].schema();
    let num_cols = schema.fields().len();
    let truncated = total > MAX_DISPLAY_ROWS;
    let half = MAX_DISPLAY_ROWS / 2;

    let display_batches = if truncated {
        let head = slice_batches(batches, 0, half);
        let tail = slice_batches(batches, total - half, half);
        head.into_iter().chain(tail).collect::<Vec<_>>()
    } else {
        batches.to_vec()
    };

    let col_names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let col_types: Vec<String> = schema
        .fields()
        .iter()
        .map(|f| friendly_type(f.data_type()))
        .collect();

    let mut columns: Vec<Vec<String>> = vec![Vec::new(); num_cols];
    for batch in &display_batches {
        for (ci, col_vec) in columns.iter_mut().enumerate() {
            col_vec.extend(column_to_strings(batch.column(ci).as_ref()));
        }
    }

    let mut widths: Vec<usize> = Vec::with_capacity(num_cols);
    for i in 0..num_cols {
        let w = col_names[i]
            .len()
            .max(col_types[i].len())
            .max(columns[i].iter().map(String::len).max().unwrap_or(0))
            .max(1);
        widths.push(w);
    }

    // Ensure table is wide enough for the footer text when truncated.
    if truncated {
        let footer_len = footer_text_len(total, half, num_cols);
        let cur_inner = inner_width(&widths);
        if cur_inner < footer_len + 2 {
            let extra = footer_len + 2 - cur_inner;
            if let Some(last) = widths.last_mut() {
                *last += extra;
            }
        }
    }

    let mut out = String::new();

    push_border(&mut out, &widths, '┌', '┬', '┐');
    push_row(&mut out, &widths, &refs(&col_names));
    push_row(&mut out, &widths, &refs(&col_types));
    push_border(&mut out, &widths, '├', '┼', '┤');

    let displayed = columns[0].len();
    for row in 0..displayed {
        if truncated && row == half {
            for _ in 0..3 {
                push_dot_row(&mut out, &widths);
            }
        }
        let items: Vec<&str> = columns.iter().map(|col| col[row].as_str()).collect();
        push_row(&mut out, &widths, &items);
    }

    if truncated {
        push_border(&mut out, &widths, '├', '┴', '┤');

        let shown = half * 2;
        let friendly = friendly_row_count(total);
        let left = format!("{total} rows ({friendly}, {shown} shown)");
        let right = format!("{num_cols} column(s)");
        push_footer_content(&mut out, &widths, &left, &right);

        push_full_border(&mut out, &widths, '└', '┘');
    } else {
        push_border(&mut out, &widths, '└', '┴', '┘');
    }

    if out.ends_with('\n') {
        out.pop();
    }
    out
}

/// Returns the total number of rows across all batches.
#[must_use]
pub fn row_count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

// ---------------------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------------------

fn slice_batches(batches: &[RecordBatch], offset: usize, length: usize) -> Vec<RecordBatch> {
    let mut result = Vec::new();
    let mut seen = 0usize;
    let mut remaining = length;

    for batch in batches {
        let n = batch.num_rows();
        if seen + n <= offset {
            seen += n;
            continue;
        }
        let skip = offset.saturating_sub(seen);
        let take = remaining.min(n - skip);
        if take > 0 {
            result.push(batch.slice(skip, take));
            remaining -= take;
        }
        seen += n;
        if remaining == 0 {
            break;
        }
    }
    result
}

fn column_to_strings(array: &dyn Array) -> Vec<String> {
    let opts = FormatOptions::default();
    ArrayFormatter::try_new(array, &opts).map_or_else(
        |_| {
            (0..array.len())
                .map(|i| {
                    if array.is_null(i) {
                        "NULL".to_string()
                    } else {
                        "?".to_string()
                    }
                })
                .collect()
        },
        |formatter| {
            (0..array.len())
                .map(|i| {
                    if array.is_null(i) {
                        "NULL".to_string()
                    } else {
                        formatter.value(i).to_string()
                    }
                })
                .collect()
        },
    )
}

fn friendly_type(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "boolean".into(),
        DataType::Int8 => "int8".into(),
        DataType::Int16 => "int16".into(),
        DataType::Int32 => "int32".into(),
        DataType::Int64 => "int64".into(),
        DataType::UInt8 => "uint8".into(),
        DataType::UInt16 => "uint16".into(),
        DataType::UInt32 => "uint32".into(),
        DataType::UInt64 => "uint64".into(),
        DataType::Float16 => "float16".into(),
        DataType::Float32 => "float".into(),
        DataType::Float64 => "double".into(),
        DataType::Utf8 | DataType::LargeUtf8 => "varchar".into(),
        DataType::Binary | DataType::LargeBinary => "blob".into(),
        DataType::Date32 | DataType::Date64 => "date".into(),
        DataType::Time32(_) | DataType::Time64(_) => "time".into(),
        DataType::Timestamp(_, _) => "timestamp".into(),
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => "decimal".into(),
        DataType::Null => "null".into(),
        other => format!("{other}"),
    }
}

#[allow(clippy::cast_precision_loss)]
fn friendly_row_count(n: usize) -> String {
    if n >= 1_000_000 {
        format!("{:.2} million", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

/// Total character count between the two outer `│` of a normal data row.
fn inner_width(widths: &[usize]) -> usize {
    // Each cell: ` ` + value(w) + ` `  = w+2
    // Separators between cells: n-1
    widths.iter().sum::<usize>() + 3 * widths.len() - 1
}

fn footer_text_len(total: usize, half: usize, num_cols: usize) -> usize {
    let shown = half * 2;
    let friendly = friendly_row_count(total);
    let left = format!("{total} rows ({friendly}, {shown} shown)");
    let right = format!("{num_cols} column(s)");
    left.len() + right.len()
}

fn refs(v: &[String]) -> Vec<&str> {
    v.iter().map(String::as_str).collect()
}

// ---------------------------------------------------------------------------
// Rendering primitives
// ---------------------------------------------------------------------------

fn push_border(out: &mut String, widths: &[usize], left: char, mid: char, right: char) {
    out.push(left);
    for (i, &w) in widths.iter().enumerate() {
        for _ in 0..w + 2 {
            out.push('─');
        }
        if i < widths.len() - 1 {
            out.push(mid);
        }
    }
    out.push(right);
    out.push('\n');
}

fn push_row(out: &mut String, widths: &[usize], values: &[&str]) {
    out.push('│');
    for (i, &w) in widths.iter().enumerate() {
        let val = if i < values.len() { values[i] } else { "" };
        out.push(' ');
        out.push_str(val);
        for _ in 0..w.saturating_sub(val.len()) {
            out.push(' ');
        }
        out.push(' ');
        out.push('│');
    }
    out.push('\n');
}

fn push_dot_row(out: &mut String, widths: &[usize]) {
    out.push('│');
    for &w in widths {
        out.push(' ');
        out.push('·');
        for _ in 0..w.saturating_sub(1) {
            out.push(' ');
        }
        out.push(' ');
        out.push('│');
    }
    out.push('\n');
}

fn push_footer_content(out: &mut String, widths: &[usize], left_text: &str, right_text: &str) {
    let total = inner_width(widths);
    out.push('│');
    out.push(' ');
    out.push_str(left_text);
    let padding = total.saturating_sub(2 + left_text.len() + right_text.len());
    for _ in 0..padding {
        out.push(' ');
    }
    out.push_str(right_text);
    out.push(' ');
    out.push('│');
    out.push('\n');
}

fn push_full_border(out: &mut String, widths: &[usize], left: char, right: char) {
    let total = inner_width(widths);
    out.push(left);
    for _ in 0..total {
        out.push('─');
    }
    out.push(right);
    out.push('\n');
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{Field, Schema};
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
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    fn make_large_batch(n: usize) -> RecordBatch {
        let ids: Vec<i32> = (0..n as i32).collect();
        let names: Vec<String> = (0..n).map(|i| format!("row_{i}")).collect();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(ids)),
                Arc::new(StringArray::from(
                    names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    // -- format_batches (plain ASCII, unchanged) ----------------------------

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
        assert!(output.contains("alice"));
    }

    #[test]
    fn test_format_batches_multiple_batches() {
        let b1 = make_batch(&[1], &["a"]);
        let b2 = make_batch(&[2], &["b"]);
        let output = format_batches(&[b1, b2]);
        assert!(output.contains("a"));
        assert!(output.contains("b"));
    }

    // -- row_count ----------------------------------------------------------

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

    // -- format_batches_truncated (box-drawing) -----------------------------

    #[test]
    fn test_truncated_empty() {
        assert_eq!(format_batches_truncated(&[]), "(0 rows)");
    }

    #[test]
    fn test_truncated_small_uses_box_drawing() {
        let batch = make_batch(&[1, 2], &["alice", "bob"]);
        let output = format_batches_truncated(&[batch]);
        assert!(output.contains('┌'), "should use box-drawing top-left");
        assert!(output.contains('│'), "should use box-drawing vertical");
        assert!(output.contains('└'), "should use box-drawing bottom-left");
        assert!(output.contains("int32"), "should show column type");
        assert!(output.contains("varchar"), "should show column type");
        assert!(output.contains("alice"));
        assert!(output.contains("bob"));
    }

    #[test]
    fn test_truncated_small_no_dots() {
        let batch = make_batch(&[1, 2], &["alice", "bob"]);
        let output = format_batches_truncated(&[batch]);
        assert!(!output.contains('·'), "small results should not truncate");
    }

    #[test]
    fn test_truncated_large_has_separator_and_footer() {
        let batch = make_large_batch(100);
        let output = format_batches_truncated(&[batch]);
        assert!(output.contains('·'), "should contain dot separator");
        assert!(output.contains("100 rows"), "should show total");
        assert!(output.contains("40 shown"), "should show displayed count");
        assert!(output.contains("2 column(s)"), "should show column count");
    }

    #[test]
    fn test_truncated_shows_head_and_tail() {
        let batch = make_large_batch(100);
        let output = format_batches_truncated(&[batch]);
        assert!(output.contains("row_0"), "should contain first row");
        assert!(output.contains("row_19"), "should contain last head row");
        assert!(output.contains("row_80"), "should contain first tail row");
        assert!(output.contains("row_99"), "should contain last row");
        assert!(!output.contains("row_40"), "should not contain middle rows");
    }

    #[test]
    fn test_truncated_at_boundary() {
        let batch = make_large_batch(MAX_DISPLAY_ROWS);
        let output = format_batches_truncated(&[batch]);
        assert!(
            !output.contains('·'),
            "exactly MAX rows should not truncate"
        );
    }

    #[test]
    fn test_truncated_just_over_boundary() {
        let batch = make_large_batch(MAX_DISPLAY_ROWS + 1);
        let output = format_batches_truncated(&[batch]);
        assert!(output.contains('·'), "MAX+1 rows should truncate");
        assert!(output.contains("41 rows"));
    }

    // -- slice_batches ------------------------------------------------------

    #[test]
    fn test_slice_batches_single() {
        let batch = make_large_batch(10);
        let sliced = slice_batches(&[batch], 3, 4);
        let total: usize = sliced.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4);
    }

    #[test]
    fn test_slice_batches_across_boundary() {
        let b1 = make_large_batch(5);
        let b2 = make_large_batch(5);
        let sliced = slice_batches(&[b1, b2], 3, 4);
        let total: usize = sliced.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 4);
    }

    // -- friendly helpers ---------------------------------------------------

    #[test]
    fn test_friendly_row_count() {
        assert_eq!(friendly_row_count(500), "500");
        assert_eq!(friendly_row_count(1_500), "1.5K");
        assert_eq!(friendly_row_count(1_260_000), "1.26 million");
    }

    #[test]
    fn test_friendly_type() {
        assert_eq!(friendly_type(&DataType::Int32), "int32");
        assert_eq!(friendly_type(&DataType::Utf8), "varchar");
        assert_eq!(friendly_type(&DataType::Float64), "double");
        assert_eq!(friendly_type(&DataType::Boolean), "boolean");
    }

    // -- rendering consistency ----------------------------------------------

    #[test]
    fn test_box_drawing_line_widths_consistent() {
        let batch = make_batch(&[1, 2], &["alice", "bob"]);
        let output = format_batches_truncated(&[batch]);
        let lines: Vec<&str> = output.lines().collect();
        let expected_width = lines[0].chars().count();
        for (i, line) in lines.iter().enumerate() {
            assert_eq!(
                line.chars().count(),
                expected_width,
                "line {i} width mismatch: {line:?}"
            );
        }
    }

    #[test]
    fn test_truncated_line_widths_consistent() {
        let batch = make_large_batch(100);
        let output = format_batches_truncated(&[batch]);
        let lines: Vec<&str> = output.lines().collect();
        let expected_width = lines[0].chars().count();
        for (i, line) in lines.iter().enumerate() {
            assert_eq!(
                line.chars().count(),
                expected_width,
                "line {i} width mismatch: {line:?}"
            );
        }
    }
}
