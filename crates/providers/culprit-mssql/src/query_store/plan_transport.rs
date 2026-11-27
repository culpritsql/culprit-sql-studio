//! Query Store sends large allowed plans as gzip. Restore the original text before accounting or retaining a row.
use super::layout::NativeLayout;
use crate::{
    read::{Parameter, invalid_data, optional_integer},
    value::{Rec, Val},
};
use culprit_core::{Result, telemetry::CollectionBudget};
use flate2::bufread::GzDecoder;
use std::io::Read;

const COMPRESS_FROM_BYTES: usize = 32 * 1024;
const ENCODED_COLUMN: &str = "culprit_plan_bytes";

pub(crate) enum Transfer {
    Text,
    GzipLargePlans,
}
impl Transfer {
    pub(super) fn for_layout(layout: Option<NativeLayout>) -> Self {
        // Boxed SQL Server with Query Store is 2016+. Azure SQL Database and Managed Instance also support COMPRESS.
        match layout.map(|layout| layout.edition) {
            Some(2..=5 | 8) => Self::GzipLargePlans,
            _ => Self::Text,
        }
    }
    pub(super) fn projection(self) -> (String, &'static str) {
        match self {
            Self::Text => ("p.query_plan".into(), "payload"),
            Self::GzipLargePlans => (
                format!(
                    "CASE WHEN DATALENGTH(p.query_plan)>={COMPRESS_FROM_BYTES} THEN COMPRESS(p.query_plan) ELSE CONVERT(varbinary(max),p.query_plan) END"
                ),
                ENCODED_COLUMN,
            ),
        }
    }
}

pub(crate) struct Decoder {
    column: usize,
    limit: usize,
}
impl Decoder {
    pub(crate) fn for_columns<'a>(
        mut names: impl Iterator<Item = &'a str>,
        parameters: &[Parameter],
    ) -> Result<Option<Self>> {
        let Some(column) = names.position(|name| name == ENCODED_COLUMN) else { return Ok(None) };
        let [Parameter::Text(_), Parameter::Integer(limit)] = parameters else { return Err(invalid_data()) };
        let limit = usize::try_from(*limit).map_err(|_| invalid_data())?;
        Ok(Some(Self { column, limit: limit.min(CollectionBudget::upper_limits().max_plan_bytes as usize) }))
    }

    pub(crate) fn decode(&self, row: &mut Rec) -> Result<()> {
        let bytes = optional_integer(row, "source_bytes")?;
        let withheld = row.bool("encrypted") || row.bool("restricted");
        let (name, value) = row.column_mut(self.column).ok_or_else(invalid_data)?;
        if let Val::Bytes(data) = value {
            let size = bytes
                .and_then(|n| usize::try_from(n).ok())
                .filter(|n| !withheld && *n <= self.limit && n.is_multiple_of(2))
                .ok_or_else(invalid_data)?;
            *value = Val::Text(decode_plan(data, size)?);
        } else if !matches!(value, Val::Null) {
            return Err(invalid_data());
        }
        *name = "payload".into();
        Ok(())
    }
}

fn decode_plan(data: &[u8], size: usize) -> Result<String> {
    let mut inflated = Vec::new();
    let bytes = if size >= COMPRESS_FROM_BYTES {
        let mut decoder = GzDecoder::new(data);
        decoder.by_ref().take(size as u64 + 1).read_to_end(&mut inflated).map_err(|_| invalid_data())?;
        if !decoder.get_ref().is_empty() {
            return Err(invalid_data());
        }
        inflated.as_slice()
    } else {
        data
    };
    if bytes.len() != size {
        return Err(invalid_data());
    }
    char::decode_utf16(bytes.as_chunks::<2>().0.iter().copied().map(u16::from_le_bytes))
        .collect::<std::result::Result<String, _>>()
        .map_err(|_| invalid_data())
}

#[cfg(test)]
mod tests;
