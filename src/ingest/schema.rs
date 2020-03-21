use std::result::Result;

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct Schema {
    pub column_names: Option<Vec<String>>,
    pub column_schemas: Vec<ColumnSchema>,
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub struct ColumnSchema {
    pub types: ColumnType,
    pub transformation: Option<ColumnTransformation>,
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum ColumnType {
    String,
    Integer,
    NullableString,
    NullableInteger,
    Drop,
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
pub enum ColumnTransformation {
    Multiply100,
    Multiply1000,
    Date,
}


impl Schema {
    pub fn parse(s: &str) -> Result<Schema, String> {
        let mut column_names = Vec::new();
        let mut column_schemas = Vec::new();
        let columns = s.split(',');
        for column in columns {
            let segments = column.split(':').collect::<Vec<_>>();
            if segments.is_empty() {
                column_schemas.push(ColumnSchema::drop_column());
            } else if segments.len() == 1 {
                column_schemas.push(ColumnSchema::parse(segments[0])?);
            } else if segments.len() == 2 {
                column_names.push(segments[0].to_string());
                column_schemas.push(ColumnSchema::parse(segments[1])?);
            } else {
                return Err(format!("Expected at most one `:` in {}.", column));
            }
        }
        if !column_names.is_empty() && column_names.len() != column_schemas.len() {
            return Err("Must specify names for all columns, or for none.".to_string());
        }
        Ok(Schema {
            column_names: if column_names.is_empty() { None } else { Some(column_names) },
            column_schemas,
        })
    }
}

impl ColumnSchema {
    fn parse(s: &str) -> Result<ColumnSchema, String> {
        let segments = s.split('.').collect::<Vec<_>>();
        let (stype, stransform) = if segments.len() == 1 {
            (segments[0].to_string(), "".to_string())
        } else if segments.len() == 2 {
            (segments[0].to_string(), segments[1].to_string())
        } else {
            return Err(format!("Expected at most one `.` in {}.", s));
        };
        let types = match stype.as_ref() {
            "integer" | "int" | "i" => ColumnType::Integer,
            "ninteger" | "nint" | "ni" => ColumnType::NullableInteger,
            "string" | "s" => ColumnType::String,
            "nstring" | "ns" => ColumnType::NullableString,
            "" => ColumnType::Drop,
            _ => return Err(format!("Unrecognized type {}.", s))
        };
        let transformation = match stransform.as_ref() {
            "date" => Some(ColumnTransformation::Date),
            "100" => Some(ColumnTransformation::Multiply100),
            "1000" => Some(ColumnTransformation::Multiply1000),
            _ => None,
        };
        Ok(ColumnSchema {
            types,
            transformation,
        })
    }

    fn drop_column() -> ColumnSchema {
        ColumnSchema {
            types: ColumnType::Drop,
            transformation: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::ingest::nyc_taxi_data::nyc_schema;

    use super::*;

    #[test]
    fn test_parse_schema() {
        let expected = Ok(Schema {
            column_names: None,
            column_schemas: vec![
                ColumnSchema { types: ColumnType::Integer, transformation: None },
                ColumnSchema { types: ColumnType::NullableString, transformation: None },
                ColumnSchema { types: ColumnType::String, transformation: None },
                ColumnSchema { types: ColumnType::NullableInteger, transformation: None },
            ],
        });
        let actual = Schema::parse("i,ns,string,nint");
        assert_eq!(expected, actual);
        assert!(Schema::parse(&nyc_schema()).is_ok(), format!("{:?}", Schema::parse(&nyc_schema())));
    }
}