use polars_core::frame::DataFrame;
use serde::de;

pub struct Deserializer<'a>(&'a DataFrame, Order);

impl<'a> Deserializer<'a> {
    pub fn columns(value: &'a DataFrame) -> Self {
        Self(value, Order::Column)
    }

    #[cfg(feature = "rows")]
    pub fn rows(value: &'a DataFrame) -> Self {
        Self(value, Order::Row)
    }
}

impl<'de, 'a> de::Deserializer<'de> for Deserializer<'a> {
    type Error = super::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.1 {
            Order::Column => visitor.visit_map(de::value::MapDeserializer::new(
                self.0.column_iter().map(|column| {
                    (
                        de::value::StrDeserializer::new(column.name()),
                        de::value::SeqDeserializer::new(
                            column.phys_iter().map(super::AnyValueDeserializer::new),
                        ),
                    )
                }),
            )),
            #[cfg(feature = "rows")]
            Order::Row => visitor.visit_seq(de::value::SeqDeserializer::new(
                (0..self.0.height()).map(|i| {
                    ResultDeserializer(
                        self.0
                            .get_row(i)
                            .map(|row| {
                                super::RowDeserializer::new(
                                    self.0.column_iter().map(|column| column.name().as_str()),
                                    row,
                                )
                            })
                            .map_err(super::Error::from),
                    )
                }),
            )),
        }
    }

    deserialize_delegate!();
}

impl<'de, 'a> de::IntoDeserializer<'de, super::Error> for Deserializer<'a> {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

pub struct BorrowedDeserializer<'de>(&'de DataFrame, Order);

impl<'de> BorrowedDeserializer<'de> {
    pub fn columns(value: &'de DataFrame) -> Self {
        Self(value, Order::Column)
    }

    #[cfg(feature = "rows")]
    pub fn rows(value: &'de DataFrame) -> Self {
        Self(value, Order::Row)
    }
}

impl<'de> de::Deserializer<'de> for BorrowedDeserializer<'de> {
    type Error = super::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        match self.1 {
            Order::Column => visitor.visit_map(de::value::MapDeserializer::new(
                self.0.column_iter().map(|column| {
                    (
                        de::value::BorrowedStrDeserializer::new(column.name()),
                        de::value::SeqDeserializer::new(
                            column
                                .phys_iter()
                                .map(super::BorrowedAnyValueDeserializer::new),
                        ),
                    )
                }),
            )),
            #[cfg(feature = "rows")]
            Order::Row => visitor.visit_seq(de::value::SeqDeserializer::new(
                (0..self.0.height()).map(|i| {
                    ResultDeserializer(
                        self.0
                            .get_row(i)
                            .map(|row| {
                                super::BorrowedRowDeserializer::new(
                                    self.0.column_iter().map(|column| column.name().as_str()),
                                    row,
                                )
                            })
                            .map_err(super::Error::from),
                    )
                }),
            )),
        }
    }

    deserialize_delegate!();
}

impl<'de> de::IntoDeserializer<'de, super::Error> for BorrowedDeserializer<'de> {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

enum Order {
    Column,
    #[cfg(feature = "rows")]
    Row,
}

#[cfg(feature = "rows")]
struct ResultDeserializer<T>(Result<T, super::Error>);

#[cfg(feature = "rows")]
impl<'de, T> de::Deserializer<'de> for ResultDeserializer<T>
where
    T: de::Deserializer<'de, Error = super::Error>,
{
    type Error = super::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        self.0?.deserialize_any(visitor)
    }

    deserialize_delegate!();
}

#[cfg(feature = "rows")]
impl<'de, T> de::IntoDeserializer<'de, super::Error> for ResultDeserializer<T>
where
    T: de::Deserializer<'de, Error = super::Error>,
{
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

#[cfg(test)]
mod tests {
    use polars_core::frame::DataFrame;
    use polars_core::frame::column::Column;
    use serde::Deserialize;

    #[test]
    fn test_columns() {
        #[derive(Debug, PartialEq, Deserialize)]
        struct Columns<'a> {
            #[serde(borrow, rename = "Ocean")]
            ocean: Vec<&'a str>,
            #[serde(rename = "Area (km²)")]
            area: Vec<u64>,
        }

        let s1 = Column::new("Ocean".into(), ["Atlantic", "Indian"]);
        let s2 = Column::new("Area (km²)".into(), [106_460_000, 70_560_000]);
        let df = DataFrame::new(vec![s1, s2]).unwrap();

        let columns = Columns::deserialize(super::BorrowedDeserializer::columns(&df)).unwrap();
        assert_eq!(
            columns,
            Columns {
                ocean: vec!["Atlantic", "Indian"],
                area: vec![106_460_000, 70_560_000],
            },
        );
    }

    #[cfg(feature = "rows")]
    #[test]
    fn test_rows() {
        #[derive(Debug, PartialEq, Deserialize)]
        struct Row<'a> {
            #[serde(rename = "Ocean")]
            ocean: &'a str,
            #[serde(rename = "Area (km²)")]
            area: u64,
        }

        let s1 = Column::new("Ocean".into(), ["Atlantic", "Indian"]);
        let s2 = Column::new("Area (km²)".into(), [106_460_000, 70_560_000]);
        let df = DataFrame::new(vec![s1, s2]).unwrap();

        let rows = Vec::<Row<'_>>::deserialize(super::BorrowedDeserializer::rows(&df)).unwrap();
        assert_eq!(
            rows,
            [
                Row {
                    ocean: "Atlantic",
                    area: 106_460_000,
                },
                Row {
                    ocean: "Indian",
                    area: 70_560_000,
                },
            ],
        );
    }
}
