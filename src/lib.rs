//!
//! ```
//! # use polars_core::frame::DataFrame;
//! # use polars_core::frame::column::Column;
//! use polars_serde::de::BorrowedDataFrameDeserializer;
//! # use serde::Deserialize;
//!
//! let s1 = Column::new("Ocean".into(), ["Atlantic", "Indian"]);
//! let s2 = Column::new("Area (km²)".into(), [106_460_000, 70_560_000]);
//! let df = DataFrame::new(vec![s1, s2])?;
//!
//! #[derive(Debug, PartialEq, Deserialize)]
//! struct Columns<'a> {
//!     #[serde(borrow, rename = "Ocean")]
//!     ocean: Vec<&'a str>,
//!     #[serde(rename = "Area (km²)")]
//!     area: Vec<u64>,
//! }
//!
//! let columns = Columns::deserialize(BorrowedDataFrameDeserializer::columns(&df))?;
//! assert_eq!(
//!     columns,
//!     Columns {
//!         ocean: vec!["Atlantic", "Indian"],
//!         area: vec![106_460_000, 70_560_000],
//!     },
//! );
//!
//! #[derive(Debug, PartialEq, Deserialize)]
//! struct Row<'a> {
//!     #[serde(rename = "Ocean")]
//!     ocean: &'a str,
//!     #[serde(rename = "Area (km²)")]
//!     area: u64,
//! }
//!
//! let rows = Vec::<Row<'_>>::deserialize(BorrowedDataFrameDeserializer::rows(&df))?;
//! assert_eq!(
//!     rows,
//!     [
//!         Row {
//!             ocean: "Atlantic",
//!             area: 106_460_000,
//!         },
//!         Row {
//!             ocean: "Indian",
//!             area: 70_560_000,
//!         },
//!     ],
//! );
//! # Ok::<_, polars_serde::de::Error>(())
//! ```

pub mod de;
