//! Bounded portable binary SQL Server Extended Events (.xel) framing and metadata decoder.
//!
//! Framing, embedded metadata catalogs (`Package`, `Event`, `Action`, `Map`, `Type`), and QPC-to-UTC session clock
//! decoding live here with zero OS, database-driver, or helper-process dependencies. Decoded events feed the shared
//! event normalizer in `super::events`.
use super::events::{Parsed, ParsedEventXml, RawEvent, normalize_raw_event};
use culprit_core::{ImportedEventLocation, NativeLogIssue, NativeLogLimits, NativeLogProblem};
use std::collections::BTreeMap;

const FILE_MAGIC: u32 = 0xEFAB_375A;
const SUPPORTED_VERSION: u16 = 10;
const MIN_BLOCK_BYTES: usize = 4096;
const METADATA_HEADER_BYTES: usize = 76;
const EVENT_HEADER_BYTES: usize = 77;
const EVENT_RECORD_HEADER_BYTES: usize = 24;
const METADATA_BLOCK_TYPE: u16 = 1;
const EVENT_BLOCK_TYPE: u16 = 0;
const EVENT_BLOCK_MARKER: u8 = 0xCC;
const END_OF_EVENT_ACTION: u32 = 0x6000_0000;
const MAX_DIRECTORY_ENTRIES: usize = 32;
const MAX_CATALOG_ITEMS: usize = 8192;
const MAX_UTF16_UNITS: usize = 64 * 1024;
const FILETIME_UNIX_EPOCH: i128 = 116_444_736_000_000_000;

#[derive(Clone, Copy)]
struct SessionClock {
    origin_filetime: u64,
    qpc_frequency: u64,
}

struct EventDef {
    package: String,
    name: String,
    fixed_columns: usize,
    columns: Vec<ColumnDef>,
}

struct ColumnDef {
    name: String,
    type_id: u32,
    offset: usize,
    flags: u16,
}

struct ActionDef {
    package: String,
    name: String,
    type_id: u32,
}

struct TypeDef {
    name: String,
    size: u8,
}

struct MapDef {
    flags: u8,
    entries: BTreeMap<u32, String>,
}

#[derive(Default)]
struct Catalog {
    clocks: BTreeMap<[u8; 16], SessionClock>,
    events: BTreeMap<u32, EventDef>,
    actions: BTreeMap<u32, ActionDef>,
    types: BTreeMap<u32, TypeDef>,
    maps: BTreeMap<u32, MapDef>,
}

/// Decode a supplied binary `.xel` buffer within `limits`. Truncated or corrupt trailing blocks stop cleanly while
/// retaining earlier verified blocks as `SourceState::Partial` in the caller.
pub fn parse_xel(input: &[u8], limits: NativeLogLimits) -> Parsed<ParsedEventXml> {
    if !limits.is_valid() {
        return Err(NativeLogProblem::InvalidLimits);
    }
    if input.len() > limits.input_bytes {
        return Err(NativeLogProblem::InputLimit);
    }
    if input.len() < 8 {
        if input.len() >= 4 && read_u32(input, 0)? != FILE_MAGIC {
            return Err(NativeLogProblem::NativeShape);
        }
        return Err(NativeLogProblem::MalformedRecord);
    }
    if read_u32(input, 0)? != FILE_MAGIC {
        return Err(NativeLogProblem::NativeShape);
    }
    let version = read_u16(input, 4)?;
    if version != SUPPORTED_VERSION {
        return Err(NativeLogProblem::UnsupportedVersion);
    }
    let header_bytes = usize::from(read_u16(input, 6)?);
    if header_bytes < MIN_BLOCK_BYTES || !header_bytes.is_multiple_of(MIN_BLOCK_BYTES) {
        return Err(NativeLogProblem::NativeShape);
    }
    if input.len() < header_bytes {
        return Err(NativeLogProblem::MalformedRecord);
    }

    let mut catalog = Catalog::default();
    let mut batch = ParsedEventXml {
        records: vec![],
        deadlocks: vec![],
        locations: vec![],
        issues: vec![],
        stopped: None,
        records_examined: 0,
    };
    let mut offset = header_bytes;
    let mut saw_block = false;

    while offset < input.len() {
        if input.len() - offset < METADATA_HEADER_BYTES {
            return finish_or_err(saw_block, &mut batch, NativeLogProblem::MalformedRecord);
        }
        let block_version = read_u16(input, offset + 12)?;
        let block_type = read_u16(input, offset + 14)?;
        let block_size = read_u32(input, offset + 16)? as usize;
        let block_hdr = usize::from(read_u16(input, offset + 22)?);
        let valid_len = read_u32(input, offset + 24)? as usize;

        if block_version != SUPPORTED_VERSION {
            let problem = if block_version == 0 && block_size == 0 {
                NativeLogProblem::MalformedRecord
            } else {
                NativeLogProblem::UnsupportedVersion
            };
            return finish_or_err(saw_block, &mut batch, problem);
        }
        if block_size < MIN_BLOCK_BYTES || !block_size.is_multiple_of(512) {
            return finish_or_err(saw_block, &mut batch, NativeLogProblem::NativeShape);
        }
        let Some(payload_end) = block_hdr.checked_add(valid_len).filter(|&end| end <= block_size) else {
            return finish_or_err(saw_block, &mut batch, NativeLogProblem::MalformedRecord);
        };
        let Some(block_end) = offset.checked_add(block_size) else {
            return finish_or_err(saw_block, &mut batch, NativeLogProblem::MalformedRecord);
        };
        if block_end > input.len() {
            return finish_or_err(saw_block, &mut batch, NativeLogProblem::MalformedRecord);
        }

        let block = &input[offset..block_end];
        match block_type {
            METADATA_BLOCK_TYPE => {
                if block_hdr != METADATA_HEADER_BYTES {
                    return finish_or_err(saw_block, &mut batch, NativeLogProblem::NativeShape);
                }
                if let Err(problem) = parse_metadata_block(&block[block_hdr..payload_end], &mut catalog) {
                    return finish_or_err(saw_block, &mut batch, problem);
                }
            }
            EVENT_BLOCK_TYPE => {
                if block_hdr != EVENT_HEADER_BYTES || block.get(METADATA_HEADER_BYTES) != Some(&EVENT_BLOCK_MARKER) {
                    return finish_or_err(saw_block, &mut batch, NativeLogProblem::NativeShape);
                }
                let runtime_guid: [u8; 16] = block
                    .get(32..48)
                    .and_then(|slice| slice.try_into().ok())
                    .ok_or(NativeLogProblem::MalformedRecord)?;
                let Some(&clock) = catalog.clocks.get(&runtime_guid) else {
                    return finish_or_err(saw_block, &mut batch, NativeLogProblem::NativeShape);
                };
                parse_event_block(
                    &block[block_hdr..payload_end],
                    offset,
                    block_hdr,
                    clock,
                    &catalog,
                    limits,
                    &mut batch,
                );
                if batch.stopped.is_some() {
                    return Ok(batch);
                }
            }
            _ => return finish_or_err(saw_block, &mut batch, NativeLogProblem::NativeShape),
        }
        saw_block = true;
        offset = block_end;
    }

    if !saw_block {
        return Err(NativeLogProblem::MalformedRecord);
    }
    Ok(batch)
}

fn finish_or_err(saw_block: bool, batch: &mut ParsedEventXml, problem: NativeLogProblem) -> Parsed<ParsedEventXml> {
    if saw_block {
        batch.stopped = Some(problem);
        Ok(std::mem::replace(
            batch,
            ParsedEventXml {
                records: vec![],
                deadlocks: vec![],
                locations: vec![],
                issues: vec![],
                stopped: None,
                records_examined: 0,
            },
        ))
    } else {
        Err(problem)
    }
}

fn parse_metadata_block(payload: &[u8], catalog: &mut Catalog) -> Parsed<()> {
    let mut pos = 0usize;
    while pos < payload.len() {
        let record_version = read_u16(payload, pos)?;
        if record_version != SUPPORTED_VERSION {
            return Err(NativeLogProblem::UnsupportedVersion);
        }
        let record_len = read_u32(payload, pos + 2)? as usize;
        if record_len < 58 || !record_len.is_multiple_of(8) {
            return Err(NativeLogProblem::MalformedRecord);
        }
        let record_end = pos.checked_add(record_len).ok_or(NativeLogProblem::MalformedRecord)?;
        let rec = payload.get(pos..record_end).ok_or(NativeLogProblem::MalformedRecord)?;
        parse_package_record(rec, catalog)?;
        pos = record_end;
    }
    Ok(())
}

fn parse_package_record(rec: &[u8], catalog: &mut Catalog) -> Parsed<()> {
    let runtime_guid: [u8; 16] =
        rec.get(6..22).and_then(|slice| slice.try_into().ok()).ok_or(NativeLogProblem::MalformedRecord)?;
    let origin_filetime = read_u64(rec, 32)?;
    let qpc_frequency = read_u64(rec, 40)?;
    if read_u16(rec, 48)? != SUPPORTED_VERSION {
        return Err(NativeLogProblem::UnsupportedVersion);
    }
    catalog.clocks.insert(runtime_guid, SessionClock { origin_filetime, qpc_frequency });

    let (pkg_name, after_name) = read_utf16z(rec, 56)?;
    let (_, after_desc) = read_utf16z(rec, after_name)?;
    let dir_offset = after_desc.checked_add(32).ok_or(NativeLogProblem::MalformedRecord)?;
    let dir_count = read_u32(rec, dir_offset + 2)? as usize;
    if dir_count > MAX_DIRECTORY_ENTRIES {
        return Err(NativeLogProblem::NativeShape);
    }
    let mut cur = dir_offset
        .checked_add(6)
        .and_then(|value| value.checked_add(dir_count.saturating_mul(12)))
        .ok_or(NativeLogProblem::MalformedRecord)?;
    if cur > rec.len() {
        return Err(NativeLogProblem::MalformedRecord);
    }

    let event_count = bounded_count(rec, &mut cur)?;
    for _ in 0..event_count {
        let _ver = read_u16(rec, cur)?;
        let _flags = read_u16(rec, cur + 2)?;
        let event_id = read_u32(rec, cur + 4)?;
        cur += 8;
        let (ename, next) = read_utf16z(rec, cur)?;
        let (_, next) = read_utf16z(rec, next)?;
        cur = next;
        let const_count = usize::from(read_u16(rec, cur)?);
        let custom_count = usize::from(read_u16(rec, cur + 2)?);
        let data_count = usize::from(read_u16(rec, cur + 4)?);
        let fixed_columns = usize::from(read_u16(rec, cur + 6)?);
        cur = cur.checked_add(8).ok_or(NativeLogProblem::MalformedRecord)?;
        if fixed_columns > data_count
            || const_count > MAX_CATALOG_ITEMS
            || custom_count > MAX_CATALOG_ITEMS
            || data_count > MAX_CATALOG_ITEMS
        {
            return Err(NativeLogProblem::NativeShape);
        }
        for _ in 0..const_count {
            let ftype = read_u32(rec, cur)?;
            cur += 4;
            let (_, next) = read_utf16z(rec, cur)?;
            let const_bytes = if (ftype & 0x00FF_FF00) == 0x004000 { 16 } else { 8 };
            let val_end = next.checked_add(const_bytes).ok_or(NativeLogProblem::MalformedRecord)?;
            if val_end > rec.len() {
                return Err(NativeLogProblem::MalformedRecord);
            }
            let (_, next) = read_utf16z(rec, val_end)?;
            cur = next;
        }
        for _ in 0..custom_count {
            let _ = read_u32(rec, cur)?;
            cur += 4;
            let (_, next) = read_utf16z(rec, cur)?;
            let val_end = next.checked_add(24).ok_or(NativeLogProblem::MalformedRecord)?;
            if val_end > rec.len() {
                return Err(NativeLogProblem::MalformedRecord);
            }
            let (_, next) = read_utf16z(rec, val_end)?;
            cur = next;
        }
        let mut columns = Vec::with_capacity(data_count);
        for _ in 0..data_count {
            let type_id = read_u32(rec, cur)?;
            cur += 4;
            let (cname, next) = read_utf16z(rec, cur)?;
            let offset = usize::from(read_u16(rec, next)?);
            let flags = read_u16(rec, next + 2)?;
            let (_, next) = read_utf16z(rec, next + 4)?;
            cur = next;
            columns.push(ColumnDef { name: cname, type_id, offset, flags });
        }
        catalog.events.insert(event_id, EventDef { package: pkg_name.clone(), name: ename, fixed_columns, columns });
    }

    let action_count = bounded_count(rec, &mut cur)?;
    for _ in 0..action_count {
        let action_id = read_u32(rec, cur + 4)?;
        cur = cur.checked_add(8).ok_or(NativeLogProblem::MalformedRecord)?;
        let (aname, next) = read_utf16z(rec, cur)?;
        let (_, next) = read_utf16z(rec, next)?;
        let type_id = read_u32(rec, next)?;
        cur = next.checked_add(12).ok_or(NativeLogProblem::MalformedRecord)?;
        if cur > rec.len() {
            return Err(NativeLogProblem::MalformedRecord);
        }
        catalog.actions.insert(action_id, ActionDef { package: pkg_name.clone(), name: aname, type_id });
    }

    let map_count = bounded_count(rec, &mut cur)?;
    for _ in 0..map_count {
        let map_id = read_u32(rec, cur + 4)?;
        cur = cur.checked_add(8).ok_or(NativeLogProblem::MalformedRecord)?;
        let (_, next) = read_utf16z(rec, cur)?;
        let (_, next) = read_utf16z(rec, next)?;
        let map_flags = *rec.get(next).ok_or(NativeLogProblem::MalformedRecord)?;
        let entry_count = usize::from(read_u16(rec, next + 1)?);
        if entry_count > MAX_CATALOG_ITEMS {
            return Err(NativeLogProblem::NativeShape);
        }
        cur = next + 3;
        let mut entries = BTreeMap::new();
        for _ in 0..entry_count {
            let key = read_u32(rec, cur)?;
            let (val, next) = read_utf16z(rec, cur + 4)?;
            cur = next;
            entries.insert(key, val);
        }
        catalog.maps.insert(map_id, MapDef { flags: map_flags, entries });
    }

    let type_count = bounded_count(rec, &mut cur)?;
    for _ in 0..type_count {
        let type_id = read_u32(rec, cur + 4)?;
        cur = cur.checked_add(8).ok_or(NativeLogProblem::MalformedRecord)?;
        let (tname, next) = read_utf16z(rec, cur)?;
        let (_, next) = read_utf16z(rec, next)?;
        let size = *rec.get(next).ok_or(NativeLogProblem::MalformedRecord)?;
        cur = next + 1;
        catalog.types.insert(type_id, TypeDef { name: tname, size });
    }

    if rec.len().saturating_sub(cur) >= 8 || cur > rec.len() {
        return Err(NativeLogProblem::MalformedRecord);
    }
    Ok(())
}

fn parse_event_block(
    ebuf: &[u8],
    block_offset: usize,
    block_hdr: usize,
    clock: SessionClock,
    catalog: &Catalog,
    limits: NativeLogLimits,
    batch: &mut ParsedEventXml,
) {
    let mut pos = 0usize;
    while pos < ebuf.len() {
        if batch.records_examined as usize == limits.records {
            batch.stopped = Some(NativeLogProblem::RecordLimit);
            return;
        }
        let event_start = pos;
        let (Ok(event_id), Ok(event_ver), Ok(qpc), Ok(payload_len)) = (
            read_u32(ebuf, pos),
            read_u16(ebuf, pos + 4),
            read_u64(ebuf, pos + 12),
            read_u32(ebuf, pos + 20).map(|len| len as usize),
        ) else {
            batch.stopped = Some(NativeLogProblem::MalformedRecord);
            return;
        };
        if event_ver != SUPPORTED_VERSION {
            batch.stopped = Some(NativeLogProblem::UnsupportedVersion);
            return;
        }
        let Some(data_end) =
            pos.checked_add(EVENT_RECORD_HEADER_BYTES).and_then(|start| start.checked_add(payload_len))
        else {
            batch.stopped = Some(NativeLogProblem::MalformedRecord);
            return;
        };
        let Some(edata) = ebuf.get(pos + EVENT_RECORD_HEADER_BYTES..data_end) else {
            batch.stopped = Some(NativeLogProblem::MalformedRecord);
            return;
        };
        pos = data_end;

        let mut raw_actions = Vec::new();
        loop {
            let (Ok(action_id), Ok(action_len)) =
                (read_u32(ebuf, pos), read_u32(ebuf, pos + 4).map(|len| len as usize))
            else {
                batch.stopped = Some(NativeLogProblem::MalformedRecord);
                return;
            };
            pos += 8;
            if action_id == END_OF_EVENT_ACTION && action_len == 0 {
                break;
            }
            let Some(aval_end) = pos.checked_add(action_len) else {
                batch.stopped = Some(NativeLogProblem::MalformedRecord);
                return;
            };
            let Some(aval) = ebuf.get(pos..aval_end) else {
                batch.stopped = Some(NativeLogProblem::MalformedRecord);
                return;
            };
            pos = aval_end;
            raw_actions.push((action_id, aval));
        }

        let position = batch.records_examined;
        batch.records_examined += 1;
        let record_bytes = pos - event_start;
        if record_bytes > limits.record_bytes {
            batch.issues.push(NativeLogIssue { position, problem: NativeLogProblem::RecordBytes });
            continue;
        }
        match decode_single_event(event_id, qpc, clock, edata, &raw_actions, catalog, position, limits.field_bytes) {
            Ok((row, graphs)) => {
                batch.records.push(row);
                batch.locations.push(ImportedEventLocation {
                    position,
                    buffer_offset: Some(block_offset as u64),
                    event_offset: (block_offset + block_hdr + event_start) as u64,
                });
                batch.deadlocks.extend(graphs.into_iter().map(|graph| (position, graph)));
            }
            Err(problem) => batch.issues.push(NativeLogIssue { position, problem }),
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn decode_single_event(
    event_id: u32,
    qpc: u64,
    clock: SessionClock,
    edata: &[u8],
    raw_actions: &[(u32, &[u8])],
    catalog: &Catalog,
    position: u32,
    field_bytes: usize,
) -> Parsed<(culprit_core::telemetry::MssqlExtendedEventObservation, Vec<culprit_core::Deadlock>)> {
    let event_def = catalog.events.get(&event_id).ok_or(NativeLogProblem::NativeShape)?;
    let mut data_fields = BTreeMap::new();
    for (idx, col) in event_def.columns.iter().enumerate() {
        let value = if idx < event_def.fixed_columns {
            let next_offset = event_def.columns.get(idx + 1).map_or(edata.len(), |next| next.offset);
            let width = catalog
                .types
                .get(&col.type_id)
                .map(|t| usize::from(t.size))
                .filter(|&size| size > 0)
                .or_else(|| next_offset.checked_sub(col.offset))
                .ok_or(NativeLogProblem::MalformedRecord)?;
            let slice = edata
                .get(col.offset..col.offset.checked_add(width).ok_or(NativeLogProblem::MalformedRecord)?)
                .ok_or(NativeLogProblem::MalformedRecord)?;
            decode_value(col.type_id, slice, catalog, field_bytes)?
        } else {
            if col.flags != 0 && col.offset.checked_add(8).is_none_or(|end| end > edata.len()) {
                continue;
            }
            let voff = read_u32(edata, col.offset)? as usize;
            let vlen = read_u32(edata, col.offset + 4)? as usize;
            let slice = edata
                .get(voff..voff.checked_add(vlen).ok_or(NativeLogProblem::MalformedRecord)?)
                .ok_or(NativeLogProblem::MalformedRecord)?;
            decode_value(col.type_id, slice, catalog, field_bytes)?
        };
        if data_fields.insert(col.name.as_str(), value).is_some() {
            return Err(NativeLogProblem::DuplicateField);
        }
    }

    let mut actions = BTreeMap::new();
    for &(action_id, aval) in raw_actions {
        let action_def = catalog.actions.get(&action_id).ok_or(NativeLogProblem::NativeShape)?;
        let decoded = decode_value(action_def.type_id, aval, catalog, field_bytes)?;
        if actions.insert(action_def.name.as_str(), (action_def.package.as_str(), decoded)).is_some() {
            return Err(NativeLogProblem::DuplicateField);
        }
    }

    normalize_raw_event(
        RawEvent {
            package: &event_def.package,
            name: &event_def.name,
            timestamp: format_qpc_utc(clock, qpc),
            data: data_fields,
            actions,
            deadlock_source: "supplied-extended-events-xel",
        },
        position,
        field_bytes,
    )
}

fn decode_value(type_id: u32, slice: &[u8], catalog: &Catalog, field_bytes: usize) -> Parsed<String> {
    if let Some(map) = catalog.maps.get(&type_id) {
        let raw = match slice.len() {
            1 => u32::from(slice[0]),
            2 => u32::from(read_u16(slice, 0)?),
            4 => read_u32(slice, 0)?,
            _ => return Err(NativeLogProblem::NativeShape),
        };
        if let Some(label) = map.entries.get(&raw) {
            return check_field_len(label.clone(), field_bytes);
        }
        if map.flags & 0x02 != 0 && raw != 0 {
            let parts: Vec<&str> = map
                .entries
                .iter()
                .filter(|(bit, _)| **bit != 0 && (raw & **bit) == **bit)
                .map(|(_, text)| text.as_str())
                .collect();
            if !parts.is_empty() {
                return check_field_len(parts.join(" | "), field_bytes);
            }
        }
        return Ok(raw.to_string());
    }
    let Some(tdef) = catalog.types.get(&type_id) else {
        if slice.len() > field_bytes {
            return Err(NativeLogProblem::FieldBytes);
        }
        return Ok(hex_bytes(slice));
    };
    let rendered = match tdef.name.as_str() {
        "int8" => (*slice.first().ok_or(NativeLogProblem::MalformedRecord)? as i8).to_string(),
        "uint8" => slice.first().ok_or(NativeLogProblem::MalformedRecord)?.to_string(),
        "boolean" => {
            if *slice.first().ok_or(NativeLogProblem::MalformedRecord)? == 0 {
                "false".to_owned()
            } else {
                "true".to_owned()
            }
        }
        "int16" => i16::from_le_bytes(slice.try_into().map_err(|_| NativeLogProblem::MalformedRecord)?).to_string(),
        "uint16" => u16::from_le_bytes(slice.try_into().map_err(|_| NativeLogProblem::MalformedRecord)?).to_string(),
        "int32" => i32::from_le_bytes(slice.try_into().map_err(|_| NativeLogProblem::MalformedRecord)?).to_string(),
        "uint32" => u32::from_le_bytes(slice.try_into().map_err(|_| NativeLogProblem::MalformedRecord)?).to_string(),
        "int64" => i64::from_le_bytes(slice.try_into().map_err(|_| NativeLogProblem::MalformedRecord)?).to_string(),
        "uint64" | "ptr" | "filetime" => {
            u64::from_le_bytes(slice.try_into().map_err(|_| NativeLogProblem::MalformedRecord)?).to_string()
        }
        "unicode_string" | "xml" => {
            if !slice.len().is_multiple_of(2) {
                return Err(NativeLogProblem::InvalidEncoding);
            }
            if slice.len() / 2 > MAX_UTF16_UNITS {
                return Err(NativeLogProblem::FieldBytes);
            }
            let (pairs, _) = slice.as_chunks::<2>();
            let units: Vec<u16> = pairs.iter().copied().map(u16::from_le_bytes).collect();
            let mut decoded = String::from_utf16(&units).map_err(|_| NativeLogProblem::InvalidEncoding)?;
            while decoded.ends_with('\0') {
                decoded.pop();
            }
            decoded
        }
        "ansi_string" => {
            if slice.len() > field_bytes {
                return Err(NativeLogProblem::FieldBytes);
            }
            let mut decoded = std::str::from_utf8(slice).map_err(|_| NativeLogProblem::InvalidUtf8)?.to_owned();
            while decoded.ends_with('\0') {
                decoded.pop();
            }
            decoded
        }
        _ => {
            if slice.len() > field_bytes {
                return Err(NativeLogProblem::FieldBytes);
            }
            hex_bytes(slice)
        }
    };
    check_field_len(rendered, field_bytes)
}

fn check_field_len(text: String, max: usize) -> Parsed<String> {
    if text.len() > max { Err(NativeLogProblem::FieldBytes) } else { Ok(text) }
}

fn hex_bytes(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(2 + bytes.len() * 2);
    out.push_str("0x");
    for byte in bytes {
        use std::fmt::Write;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn format_qpc_utc(clock: SessionClock, qpc: u64) -> Option<String> {
    if clock.origin_filetime == 0 || clock.qpc_frequency == 0 {
        return None;
    }
    let delta_100ns = (u128::from(qpc) * 10_000_000) / u128::from(clock.qpc_frequency);
    let total_ft = i128::from(clock.origin_filetime).checked_add(i128::try_from(delta_100ns).ok()?)?;
    let unix_100ns = total_ft.checked_sub(FILETIME_UNIX_EPOCH)?;
    let total_secs = i64::try_from(unix_100ns.div_euclid(10_000_000)).ok()?;
    let frac_100ns = unix_100ns.rem_euclid(10_000_000) as u32;
    let days = total_secs.div_euclid(86_400);
    let sec_of_day = total_secs.rem_euclid(86_400) as u32;
    let (year, month, day) = civil_from_days(days);
    if !(1970..=9999).contains(&year) {
        return None;
    }
    let hour = sec_of_day / 3600;
    let minute = (sec_of_day % 3600) / 60;
    let second = sec_of_day % 60;
    Some(format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{frac_100ns:07}Z"))
}

fn civil_from_days(days_since_1970: i64) -> (i64, u32, u32) {
    let z = days_since_1970 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = i64::from(yoe) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn bounded_count(buf: &[u8], cur: &mut usize) -> Parsed<usize> {
    let count = read_u32(buf, *cur)? as usize;
    *cur = cur.checked_add(4).ok_or(NativeLogProblem::MalformedRecord)?;
    if count > MAX_CATALOG_ITEMS {
        return Err(NativeLogProblem::NativeShape);
    }
    Ok(count)
}

fn read_u16(buf: &[u8], pos: usize) -> Parsed<u16> {
    let bytes: [u8; 2] = buf
        .get(pos..pos.checked_add(2).ok_or(NativeLogProblem::MalformedRecord)?)
        .and_then(|slice| slice.try_into().ok())
        .ok_or(NativeLogProblem::MalformedRecord)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u32(buf: &[u8], pos: usize) -> Parsed<u32> {
    let bytes: [u8; 4] = buf
        .get(pos..pos.checked_add(4).ok_or(NativeLogProblem::MalformedRecord)?)
        .and_then(|slice| slice.try_into().ok())
        .ok_or(NativeLogProblem::MalformedRecord)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u64(buf: &[u8], pos: usize) -> Parsed<u64> {
    let bytes: [u8; 8] = buf
        .get(pos..pos.checked_add(8).ok_or(NativeLogProblem::MalformedRecord)?)
        .and_then(|slice| slice.try_into().ok())
        .ok_or(NativeLogProblem::MalformedRecord)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_utf16z(buf: &[u8], start: usize) -> Parsed<(String, usize)> {
    let mut pos = start;
    let mut units = Vec::new();
    while let Some(pair) = buf.get(pos..pos.checked_add(2).ok_or(NativeLogProblem::MalformedRecord)?) {
        let unit = u16::from_le_bytes([pair[0], pair[1]]);
        pos += 2;
        if unit == 0 {
            let text = String::from_utf16(&units).map_err(|_| NativeLogProblem::InvalidEncoding)?;
            return Ok((text, pos));
        }
        if units.len() >= MAX_UTF16_UNITS {
            return Err(NativeLogProblem::FieldBytes);
        }
        units.push(unit);
    }
    Err(NativeLogProblem::MalformedRecord)
}

#[cfg(test)]
mod tests;
