#[macro_use]
mod macros;
mod any_value;
#[cfg(feature = "rows")]
mod data_frame;
#[cfg(feature = "rows")]
mod row;

pub use any_value::{
    BorrowedDeserializer as BorrowedAnyValueDeserializer, Deserializer as AnyValueDeserializer,
};
#[cfg(feature = "rows")]
pub use data_frame::{
    BorrowedDeserializer as BorrowedDataFrameDeserializer, Deserializer as DataFrameDeserializer,
};
use polars_core::datatypes::AnyValue;
#[cfg(feature = "rows")]
pub use row::{BorrowedDeserializer as BorrowedRowDeserializer, Deserializer as RowDeserializer};
use serde::de;
use std::fmt;
#[cfg(feature = "dtype-categorical")]
use std::sync::Arc;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Polars(#[from] polars_core::error::PolarsError),
    #[cfg(feature = "dtype-categorical")]
    #[error("invalid categorical id")]
    InvalidCategoricalId(
        polars_core::datatypes::CatSize,
        Arc<polars_core::datatypes::CategoricalMapping>,
    ),
    #[error("unknown data type")]
    UnknownDataType(AnyValue<'static>),
    #[error("{0}")]
    Custom(String),
}

impl de::Error for Error {
    fn custom<T>(m: T) -> Self
    where
        T: fmt::Display,
    {
        Self::Custom(m.to_string())
    }
}
