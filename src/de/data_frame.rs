use polars_core::error::PolarsResult;
use polars_core::frame::DataFrame;
use polars_core::frame::column::Column;
use polars_core::frame::row::Row;
use serde::de;

pub struct Deserializer<'a>(&'a DataFrame);

impl<'a> Deserializer<'a> {
    pub fn new(value: &'a DataFrame) -> Self {
        Self(value)
    }
}

impl<'de, 'a> de::Deserializer<'de> for Deserializer<'a> {
    type Error = super::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        struct Deserializer<'a, I>(I, PolarsResult<Row<'a>>);

        impl<'de, 'a, I> de::Deserializer<'de> for Deserializer<'a, I>
        where
            I: Iterator<Item = &'a Column>,
        {
            type Error = super::Error;

            fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: de::Visitor<'de>,
            {
                let row = self.1?;
                visitor.visit_map(de::value::MapDeserializer::new(self.0.zip(row.0).map(
                    |(k, v)| {
                        (
                            de::value::StrDeserializer::new(k.name()),
                            super::AnyValueDeserializer::new(v),
                        )
                    },
                )))
            }

            deserialize_delegate!();
        }

        impl<'de, 'a, I> de::IntoDeserializer<'de, super::Error> for Deserializer<'a, I>
        where
            I: Iterator<Item = &'a Column>,
        {
            type Deserializer = Self;

            fn into_deserializer(self) -> Self::Deserializer {
                self
            }
        }

        visitor.visit_seq(de::value::SeqDeserializer::new(
            (0..self.0.height()).map(|i| Deserializer(self.0.column_iter(), self.0.get_row(i))),
        ))
    }

    deserialize_delegate!();
}

impl<'de, 'a> de::IntoDeserializer<'de, super::Error> for Deserializer<'a> {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

pub struct BorrowedDeserializer<'de>(&'de DataFrame);

impl<'de> BorrowedDeserializer<'de> {
    pub fn new(value: &'de DataFrame) -> Self {
        Self(value)
    }
}

impl<'de> de::Deserializer<'de> for BorrowedDeserializer<'de> {
    type Error = super::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        struct BorrowedDeserializer<'de, I>(I, PolarsResult<Row<'de>>);

        impl<'de, I> de::Deserializer<'de> for BorrowedDeserializer<'de, I>
        where
            I: Iterator<Item = &'de Column>,
        {
            type Error = super::Error;

            fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
            where
                V: de::Visitor<'de>,
            {
                let row = self.1?;
                visitor.visit_map(de::value::MapDeserializer::new(self.0.zip(row.0).map(
                    |(k, v)| {
                        (
                            de::value::BorrowedStrDeserializer::new(k.name()),
                            super::BorrowedAnyValueDeserializer::new(v),
                        )
                    },
                )))
            }

            deserialize_delegate!();
        }

        impl<'de, I> de::IntoDeserializer<'de, super::Error> for BorrowedDeserializer<'de, I>
        where
            I: Iterator<Item = &'de Column>,
        {
            type Deserializer = Self;

            fn into_deserializer(self) -> Self::Deserializer {
                self
            }
        }

        visitor
            .visit_seq(de::value::SeqDeserializer::new((0..self.0.height()).map(
                |i| BorrowedDeserializer(self.0.column_iter(), self.0.get_row(i)),
            )))
    }

    deserialize_delegate!();
}

impl<'de> de::IntoDeserializer<'de, super::Error> for BorrowedDeserializer<'de> {
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
    fn test() {
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

        let rows = Vec::<Row<'_>>::deserialize(super::BorrowedDeserializer::new(&df)).unwrap();
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
        )
    }
}
