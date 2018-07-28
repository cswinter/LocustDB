use std::collections::HashMap;
use std::fmt;
use unit_fmt::*;

pub struct MemTreeTable {
    pub name: String,
    pub size_bytes: usize,
    pub rows: usize,
    pub fully_resident: bool,
    pub columns: HashMap<String, MemTreeColumn>,
}

pub struct MemTreeColumn {
    pub name: String,
    pub size_bytes: usize,
    pub size_percentage: f64,
    pub rows: usize,
    pub rows_percentage: f64,
    pub encodings: HashMap<String, MemTreeEncoding>,
}

#[derive(Default)]
pub struct MemTreeEncoding {
    pub codec: String,
    pub size_bytes: usize,
    pub size_column_percentage: f64,
    pub size_table_percentage: f64,
    pub rows: usize,
    pub rows_column_percentage: f64,
    pub rows_table_percentage: f64,
    pub sections: Vec<MemTreeSection>,
}

pub struct MemTreeSection {
    pub id: usize,
    pub size_bytes: usize,
}

impl MemTreeTable {
    pub fn aggregate(&mut self) {
        self.size_bytes = self.columns.values().map(|x| x.size_bytes).sum();
        for col in self.columns.values_mut() {
            col.aggregate(self.size_bytes as f64, self.rows as f64);
            self.fully_resident &= col.rows_percentage == 100.0;
        }
    }
}

impl MemTreeColumn {
    pub fn aggregate(&mut self, table_size: f64, table_rows: f64) {
        self.size_percentage = 100.0 * self.size_bytes as f64 / table_size;
        self.rows_percentage = 100.0 * self.rows as f64 / table_rows;
        for encoding in self.encodings.values_mut() {
            encoding.aggregate(self.size_bytes as f64, table_size, self.rows as f64, table_rows);
        }
    }
}

impl MemTreeEncoding {
    pub fn aggregate(&mut self, column_size: f64, table_size: f64, column_rows: f64, table_rows: f64) {
        self.size_column_percentage = 100.0 * self.size_bytes as f64 / column_size;
        self.size_table_percentage = 100.0 * self.size_bytes as f64 / table_size;
        self.rows_column_percentage = 100.0 * self.rows as f64 / column_rows;
        self.rows_table_percentage = 100.0 * self.rows as f64 / table_rows;
    }
}


impl fmt::Display for MemTreeTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}  {}  {} rows",
               self.name,
               bite(self.size_bytes),
               self.rows, )?;
        if self.fully_resident {
            write!(f, "  {:.2}/row", byte(self.size_bytes as f64 / self.rows as f64))?;
        };
        let mut columns = self.columns.values().collect::<Vec<_>>();
        columns.sort_by_key(|x| &x.name);
        for (i, column) in columns.iter().enumerate() {
            let terminal = i == self.columns.len() - 1;
            let branch = if terminal { "└─" } else { "├─" };
            let continuation = if terminal { "  " } else { "│ " };
            let mut prefix = branch;
            for line in format!("{}", column).split("\n") {
                write!(f, "\n{} {}", prefix, line)?;
                prefix = continuation;
            }
        }
        Ok(())
    }
}

impl fmt::Display for MemTreeColumn {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let residency = if self.rows == 0 {
            "nonresident".to_string()
        } else if self.rows_percentage == 100.0 {
            format!("fully resident")
        } else {
            format!("{:.2} resident", percent(self.rows_percentage))
        };
        write!(f, "{:24} {:14}  ", self.name, residency)?;
        if self.rows > 0 {
            write!(f, "{:>8} {:>5}  {:>6}/row",
                   format!("{:.2}", bite(self.size_bytes)),
                   format!("{:.2}", percent(self.size_percentage)),
                   format!("{:.2}", byte(self.size_bytes as f64 / self.rows as f64)))?;
        }

        let mut encodings = self.encodings.values().collect::<Vec<_>>();
        encodings.sort_by_key(|e| -(e.size_bytes as isize));
        for (i, encoding) in encodings.iter().enumerate() {
            let terminal = i == self.encodings.len() - 1;
            let branch = if terminal { "└─" } else { "├─" };
            let continuation = if terminal { "  " } else { "│ " };
            let mut prefix = branch;
            for line in format!("{}", encoding).split("\n") {
                write!(f, "\n{} {}", prefix, line)?;
                prefix = continuation;
            }
        }
        Ok(())
    }
}

impl fmt::Display for MemTreeEncoding {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:36}  {:>8} {:>5}  {:>6}/row  {:.2}",
               self.codec,
               format!("{:.2}", bite(self.size_bytes)),
               format!("{:.2}", percent(self.size_column_percentage)),
               format!("{:.2}", byte(self.size_bytes as f64 / self.rows as f64)),
               percent(self.rows_column_percentage), )?;
        for (i, section) in self.sections.iter().enumerate() {
            if i == self.sections.len() - 1 {
                write!(f, "\n└─ {}", section)?;
            } else {
                write!(f, "\n├─ {}", section)?;
            }
        }
        Ok(())
    }
}

impl fmt::Display for MemTreeSection {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, ".{} {:30}  {:>8}", self.id, "", format!("{:.2}", bite(self.size_bytes)))
    }
}

