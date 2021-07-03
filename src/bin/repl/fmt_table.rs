pub fn fmt_table(headings: &[&str], rows: &[Vec<&str>]) -> String {
    let ncols = headings.len();
    let mut col_width = Vec::<usize>::with_capacity(ncols);
    for heading in headings {
        col_width.push(heading.chars().count() + 1);
    }
    for row in rows {
        assert_eq!(ncols, row.len());
        for (i, entry) in row.iter().enumerate() {
            let width = entry.chars().count() + 1;
            if col_width[i] < width {
                col_width[i] = width;
            }
        }
    }

    let mut result = String::new();
    append_row(&mut result, headings, &col_width);

    result.push('\n');
    for (i, width) in col_width.iter().enumerate() {
        result.push_str(&String::from_utf8(vec![b'-'; *width]).unwrap());
        if i < ncols - 1 {
            result.push_str("+-");
        }
    }

    for row in rows {
        result.push('\n');
        append_row(&mut result, row, &col_width);
    }

    result
}

fn append_row(string: &mut String, row: &[&str], col_width: &[usize]) {
    let imax = col_width.len() - 1;
    for (i, entry) in row.iter().enumerate() {
        string.push_str(&format!("{:1$}", entry, col_width[i]));
        if i < imax {
            string.push_str("| ");
        }
    }
}
