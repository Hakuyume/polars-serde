use polars_core::datatypes::AnyValue;
use serde::de;

macro_rules! deserialize_any {
    ($visit_str:ident, $visit_bytes:ident) => {
        fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
        where
            V: de::Visitor<'de>,
        {
            match self.0 {
                AnyValue::Null => visitor.visit_none(),
                AnyValue::Boolean(v) => visitor.visit_bool(v),
                AnyValue::String(v) => visitor.$visit_str(v),
                AnyValue::UInt8(v) => visitor.visit_u8(v),
                AnyValue::UInt16(v) => visitor.visit_u16(v),
                AnyValue::UInt32(v) => visitor.visit_u32(v),
                AnyValue::UInt64(v) => visitor.visit_u64(v),
                AnyValue::Int8(v) => visitor.visit_i8(v),
                AnyValue::Int16(v) => visitor.visit_i16(v),
                AnyValue::Int32(v) => visitor.visit_i32(v),
                AnyValue::Int64(v) => visitor.visit_i64(v),
                AnyValue::Int128(v) => visitor.visit_i128(v),
                AnyValue::Float32(v) => visitor.visit_f32(v),
                AnyValue::Float64(v) => visitor.visit_f64(v),
                #[cfg(feature = "dtype-categorical")]
                AnyValue::Categorical(cat, categorical_mapping)
                | AnyValue::Enum(cat, categorical_mapping) => {
                    if let Some(v) = categorical_mapping.cat_to_str(cat) {
                        visitor.$visit_str(v)
                    } else {
                        Err(super::Error::InvalidCategoricalId(
                            cat,
                            categorical_mapping.clone(),
                        ))
                    }
                }
                #[cfg(feature = "dtype-categorical")]
                AnyValue::CategoricalOwned(cat, categorical_mapping)
                | AnyValue::EnumOwned(cat, categorical_mapping) => {
                    if let Some(v) = categorical_mapping.cat_to_str(cat) {
                        visitor.visit_str(v)
                    } else {
                        Err(super::Error::InvalidCategoricalId(cat, categorical_mapping))
                    }
                }
                AnyValue::List(v) => visitor.visit_seq(de::value::SeqDeserializer::new(
                    v.iter().map(Deserializer::new),
                )),
                #[cfg(feature = "dtype-array")]
                AnyValue::Array(v, _) => visitor.visit_seq(de::value::SeqDeserializer::new(
                    v.iter().map(Deserializer::new),
                )),
                AnyValue::StringOwned(v) => visitor.visit_string(v.into_string()),
                AnyValue::Binary(v) => visitor.$visit_bytes(v),
                AnyValue::BinaryOwned(v) => visitor.visit_byte_buf(v),
                #[allow(unreachable_patterns)]
                _ => Err(super::Error::UnknownDataType(self.0.into_static())),
            }
        }
    };
}

pub struct Deserializer<'a>(AnyValue<'a>);

impl<'a> Deserializer<'a> {
    pub fn new(value: AnyValue<'a>) -> Self {
        Self(value)
    }
}

impl<'de, 'a> de::Deserializer<'de> for Deserializer<'a> {
    type Error = super::Error;
    deserialize_any!(visit_str, visit_bytes);
    deserialize_delegate!();
}

impl<'de, 'a> de::IntoDeserializer<'de, super::Error> for Deserializer<'a> {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

pub struct BorrowedDeserializer<'de>(pub AnyValue<'de>);

impl<'de> BorrowedDeserializer<'de> {
    pub fn new(value: AnyValue<'de>) -> Self {
        Self(value)
    }
}

impl<'de> de::Deserializer<'de> for BorrowedDeserializer<'de> {
    type Error = super::Error;
    deserialize_any!(visit_borrowed_str, visit_borrowed_bytes);
    deserialize_delegate!();
}

impl<'de> de::IntoDeserializer<'de, super::Error> for BorrowedDeserializer<'de> {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}
