use std::fmt;
use std::str::FromStr;

use crate::subscriber::Offset;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer};

impl<'de> Deserialize<'de> for Offset {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(OffsetVisitor)
    }
}

struct OffsetVisitor;

impl<'de> Visitor<'de> for OffsetVisitor {
    type Value = Offset;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an offset")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Offset::from_str(value).map_err(|err| E::custom(err.to_string()))
    }
}
