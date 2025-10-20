use polars_core::frame::row::Row;
use serde::de;

pub struct Deserializer<'a, I>(I, Row<'a>);

impl<'a, I> Deserializer<'a, I> {
    pub fn new(column_names: I, value: Row<'a>) -> Self {
        Self(column_names, value)
    }
}

impl<'de, 'a, 'b, I> de::Deserializer<'de> for Deserializer<'a, I>
where
    I: IntoIterator<Item = &'b str>,
{
    type Error = super::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_map(de::value::MapDeserializer::new(
            self.0.into_iter().zip(self.1.0).map(|(k, v)| {
                (
                    de::value::StrDeserializer::new(k),
                    super::AnyValueDeserializer::new(v),
                )
            }),
        ))
    }

    deserialize_delegate!();
}

impl<'de, 'a, 'b, I> de::IntoDeserializer<'de, super::Error> for Deserializer<'a, I>
where
    I: IntoIterator<Item = &'b str>,
{
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

pub struct BorrowedDeserializer<'de, I>(I, Row<'de>);

impl<'de, I> BorrowedDeserializer<'de, I> {
    pub fn new(column_names: I, value: Row<'de>) -> Self {
        Self(column_names, value)
    }
}

impl<'de, I> de::Deserializer<'de> for BorrowedDeserializer<'de, I>
where
    I: IntoIterator<Item = &'de str>,
{
    type Error = super::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_map(de::value::MapDeserializer::new(
            self.0.into_iter().zip(self.1.0).map(|(k, v)| {
                (
                    de::value::BorrowedStrDeserializer::new(k),
                    super::BorrowedAnyValueDeserializer(v),
                )
            }),
        ))
    }

    deserialize_delegate!();
}

impl<'de, I> de::IntoDeserializer<'de, super::Error> for BorrowedDeserializer<'de, I>
where
    I: IntoIterator<Item = &'de str>,
{
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}
