use polars_core::error::PolarsResult;
use polars_core::frame::DataFrame;
use polars_core::frame::column::Column;
use polars_core::frame::row::Row;
use serde::de;

pub struct Deserializer<'a>(pub &'a DataFrame);

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
                            super::AnyValueDeserializer(v),
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

pub struct BorrowedDeserializer<'de>(pub &'de DataFrame);

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
                            super::BorrowedAnyValueDeserializer(v),
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
