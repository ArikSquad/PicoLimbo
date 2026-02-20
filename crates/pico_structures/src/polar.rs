use crate::internal_block_entity::BlockEntity;
use crate::pack_direct::pack_direct;
use crate::palette::Palette;
use crate::world::{LightSection, World};
use blocks_report::{BlockStateLookup, InternalId, InternalMapping};
use minecraft_protocol::prelude::{Coordinates, VarInt};
use pico_binutils::prelude::{BinaryReader, BinaryReaderError};
use std::path::Path;
use thiserror::Error;
use tracing::warn;

const POLAR_MAGIC_NUMBER: i32 = 0x506F6C72; // Polr
const POLAR_LATEST_VERSION: i16 = 7;
const POLAR_VERSION_UNIFIED_LIGHT: i16 = 1;
const POLAR_VERSION_USERDATA_OPT_BLOCK_ENT_NBT: i16 = 2;
const POLAR_VERSION_MINESTOM_NBT_READ_BREAK: i16 = 3;
const POLAR_VERSION_WORLD_USERDATA: i16 = 4;
const POLAR_VERSION_DATA_CONVERTER: i16 = 6;
const POLAR_VERSION_IMPROVED_LIGHT: i16 = 7;

const POLAR_BLOCK_PALETTE_SIZE: usize = 16 * 16 * 16;
const LIGHT_SECTION_BYTES: usize = 2048;

#[derive(Debug, Error)]
pub enum PolarError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    BinaryReader(#[from] BinaryReaderError),
    #[error("invalid Polar file: {0}")]
    InvalidFormat(&'static str),
    #[error("unsupported Polar version {found}; max supported is {max_supported}")]
    UnsupportedVersion { found: i16, max_supported: i16 },
    #[error("failed to decompress zstd data: {0}")]
    Zstd(String),
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum CompressionType {
    None,
    Zstd,
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum LightContent {
    Missing,
    Empty,
    Full,
    Present,
}

struct ParsedChunk {
    x: i32,
    z: i32,
    sections: Vec<Palette>,
    sky_light_sections: Vec<LightSection>,
    block_light_sections: Vec<LightSection>,
    block_entities: Vec<BlockEntity>,
}

pub fn read_polar_world(
    path: &Path,
    internal_mapping: &InternalMapping,
) -> Result<World, PolarError> {
    let bytes = std::fs::read(path)?;
    let mut reader = BinaryReader::new(&bytes);

    let magic = reader.read::<i32>()?;
    if magic != POLAR_MAGIC_NUMBER {
        return Err(PolarError::InvalidFormat("invalid magic number"));
    }

    let version = reader.read::<i16>()?;
    if version > POLAR_LATEST_VERSION {
        return Err(PolarError::UnsupportedVersion {
            found: version,
            max_supported: POLAR_LATEST_VERSION,
        });
    }

    if version >= POLAR_VERSION_DATA_CONVERTER {
        let _ = reader.read::<VarInt>()?;
    }

    let compression = match reader.read::<i8>()? {
        0 => CompressionType::None,
        1 => CompressionType::Zstd,
        _ => return Err(PolarError::InvalidFormat("invalid compression type")),
    };

    let decompressed_length = read_var_int(&mut reader)?;
    if decompressed_length < 0 {
        return Err(PolarError::InvalidFormat(
            "negative decompressed payload length",
        ));
    }

    let payload = reader.remaining_bytes()?;
    let payload = match compression {
        CompressionType::None => payload,
        CompressionType::Zstd => zstd::bulk::decompress(&payload, decompressed_length as usize)
            .map_err(|err| PolarError::Zstd(err.to_string()))?,
    };

    let mut payload_reader = BinaryReader::new(&payload);

    let min_section = payload_reader.read::<i8>()? as i32;
    let max_section = payload_reader.read::<i8>()? as i32;
    if min_section > max_section {
        return Err(PolarError::InvalidFormat("invalid section range"));
    }

    if version > POLAR_VERSION_WORLD_USERDATA {
        let _ = read_byte_array(&mut payload_reader)?;
    }

    let section_count = (max_section - min_section + 1) as usize;
    let chunk_count = read_var_int(&mut payload_reader)?;
    if chunk_count < 0 {
        return Err(PolarError::InvalidFormat("negative chunk count"));
    }

    let block_lookup = BlockStateLookup::new(internal_mapping);
    let air = lookup_air_id(&block_lookup)?;

    let mut chunks = Vec::with_capacity(chunk_count as usize);
    for _ in 0..chunk_count {
        chunks.push(read_chunk(
            &mut payload_reader,
            version,
            section_count,
            min_section,
            &block_lookup,
            air,
        )?);
    }

    if chunks.is_empty() {
        return Ok(World::from_parts(
            vec![],
            Coordinates::new(0, 0, 0),
            vec![],
            vec![],
            vec![],
            0,
            min_section,
            0,
        ));
    }

    let min_chunk_x = chunks.iter().map(|c| c.x).min().unwrap_or(0);
    let max_chunk_x = chunks.iter().map(|c| c.x).max().unwrap_or(0);
    let min_chunk_z = chunks.iter().map(|c| c.z).min().unwrap_or(0);
    let max_chunk_z = chunks.iter().map(|c| c.z).max().unwrap_or(0);

    let size_x = max_chunk_x - min_chunk_x + 1;
    let size_z = max_chunk_z - min_chunk_z + 1;
    let size_y = section_count as i32;

    let size_in_chunks = Coordinates::new(size_x, size_y, size_z);
    let section_total = (size_x * size_y * size_z) as usize;
    let chunk_column_total = (size_x * size_z) as usize;

    let mut world_sections = std::iter::repeat_with(|| Palette::single(air))
        .take(section_total)
        .collect::<Vec<_>>();
    let mut block_entities_by_chunk: Vec<Vec<BlockEntity>> = vec![Vec::new(); chunk_column_total];
    let mut sky_light_by_chunk = vec![vec![empty_light(); section_count]; chunk_column_total];
    let mut block_light_by_chunk = vec![vec![empty_light(); section_count]; chunk_column_total];

    for chunk in chunks {
        let local_x = (chunk.x - min_chunk_x) as usize;
        let local_z = (chunk.z - min_chunk_z) as usize;

        for (local_y, section) in chunk.sections.into_iter().enumerate() {
            let index =
                local_z + local_y * size_z as usize + local_x * size_y as usize * size_z as usize;
            world_sections[index] = section;
        }

        let column_index = local_z + local_x * size_z as usize;
        block_entities_by_chunk[column_index] = chunk.block_entities;
        sky_light_by_chunk[column_index] = chunk.sky_light_sections;
        block_light_by_chunk[column_index] = chunk.block_light_sections;
    }

    Ok(World::from_parts(
        world_sections,
        size_in_chunks,
        block_entities_by_chunk,
        sky_light_by_chunk,
        block_light_by_chunk,
        min_chunk_x,
        min_section,
        min_chunk_z,
    ))
}

fn read_chunk(
    reader: &mut BinaryReader,
    version: i16,
    section_count: usize,
    min_section: i32,
    block_lookup: &BlockStateLookup,
    air: InternalId,
) -> Result<ParsedChunk, PolarError> {
    let chunk_x = read_var_int(reader)?;
    let chunk_z = read_var_int(reader)?;

    let mut sections = Vec::with_capacity(section_count);
    let mut sky_light_sections = Vec::with_capacity(section_count);
    let mut block_light_sections = Vec::with_capacity(section_count);

    for _ in 0..section_count {
        let parsed = read_section(reader, version, block_lookup, air)?;
        sections.push(parsed.palette);
        sky_light_sections.push(parsed.sky_light);
        block_light_sections.push(parsed.block_light);
    }

    let block_entity_count = read_var_int(reader)?;
    if block_entity_count < 0 {
        return Err(PolarError::InvalidFormat("negative block entity count"));
    }

    let mut block_entities = Vec::new();
    for _ in 0..block_entity_count {
        let _ = reader.read::<i32>()?;
        let _ = read_optional_string(reader)?;

        let has_nbt = if version <= POLAR_VERSION_USERDATA_OPT_BLOCK_ENT_NBT {
            true
        } else {
            read_bool(reader)?
        };

        if has_nbt {
            let legacy_named_nbt = version <= POLAR_VERSION_MINESTOM_NBT_READ_BREAK;
            skip_nbt(reader, legacy_named_nbt)?;
        }
    }

    let _ = read_heightmaps(reader)?;

    if version > POLAR_VERSION_USERDATA_OPT_BLOCK_ENT_NBT {
        let _ = read_byte_array(reader)?;
    }

    if block_entities.is_empty() {
        block_entities.shrink_to_fit();
    }

    let _ = min_section;

    Ok(ParsedChunk {
        x: chunk_x,
        z: chunk_z,
        sections,
        sky_light_sections,
        block_light_sections,
        block_entities,
    })
}

struct ParsedSection {
    palette: Palette,
    sky_light: LightSection,
    block_light: LightSection,
}

fn read_section(
    reader: &mut BinaryReader,
    version: i16,
    block_lookup: &BlockStateLookup,
    air: InternalId,
) -> Result<ParsedSection, PolarError> {
    if read_bool(reader)? {
        return Ok(ParsedSection {
            palette: Palette::single(air),
            sky_light: empty_light(),
            block_light: empty_light(),
        });
    }

    let block_palette = read_string_list(reader)?;
    if block_palette.is_empty() {
        return Err(PolarError::InvalidFormat("empty block palette"));
    }

    let block_state_ids: Vec<InternalId> = block_palette
        .iter()
        .map(|state| parse_block_state(block_lookup, state, air))
        .collect();

    let block_indices = if block_state_ids.len() == 1 {
        None
    } else {
        let packed = read_long_array(reader)?;
        unpack_palette_indices(&packed, block_state_ids.len(), POLAR_BLOCK_PALETTE_SIZE)?
    };

    let palette = section_palette(&block_state_ids, block_indices.as_deref(), air);

    let biome_palette = read_string_list(reader)?;
    if biome_palette.len() > 1 {
        let _ = read_long_array(reader)?;
    }

    let (block_light_content, block_light_data, sky_light_content, sky_light_data) =
        read_lighting(reader, version)?;

    Ok(ParsedSection {
        palette,
        sky_light: light_to_section(sky_light_content, sky_light_data),
        block_light: light_to_section(block_light_content, block_light_data),
    })
}

fn read_lighting(
    reader: &mut BinaryReader,
    version: i16,
) -> Result<(LightContent, Option<Vec<i8>>, LightContent, Option<Vec<i8>>), PolarError> {
    let mut block_content = LightContent::Missing;
    let mut block_data = None;
    let mut sky_content = LightContent::Missing;
    let mut sky_data = None;

    if version > POLAR_VERSION_UNIFIED_LIGHT {
        block_content = if version >= POLAR_VERSION_IMPROVED_LIGHT {
            read_light_content(reader)?
        } else if read_bool(reader)? {
            LightContent::Present
        } else {
            LightContent::Missing
        };

        if block_content == LightContent::Present {
            block_data = Some(read_fixed_light_data(reader)?);
        }

        sky_content = if version >= POLAR_VERSION_IMPROVED_LIGHT {
            read_light_content(reader)?
        } else if read_bool(reader)? {
            LightContent::Present
        } else {
            LightContent::Missing
        };

        if sky_content == LightContent::Present {
            sky_data = Some(read_fixed_light_data(reader)?);
        }
    } else if read_bool(reader)? {
        block_content = LightContent::Present;
        block_data = Some(read_fixed_light_data(reader)?);
        sky_content = LightContent::Present;
        sky_data = Some(read_fixed_light_data(reader)?);
    }

    Ok((block_content, block_data, sky_content, sky_data))
}

fn parse_block_state(block_lookup: &BlockStateLookup, raw: &str, air: InternalId) -> InternalId {
    match block_lookup
        .parse_state_string(raw)
        .or_else(|_| block_lookup.parse_state_string(&normalize_block_name(raw)))
    {
        Ok(state) => state.internal_id(),
        Err(err) => {
            warn!("Failed to parse Polar block state '{raw}': {err}. Falling back to air.");
            air
        }
    }
}

fn normalize_block_name(input: &str) -> String {
    if input.contains(':') {
        input.to_string()
    } else {
        format!("minecraft:{input}")
    }
}

fn lookup_air_id(block_lookup: &BlockStateLookup) -> Result<InternalId, PolarError> {
    block_lookup
        .parse_state_string("minecraft:air")
        .map(|state| state.internal_id())
        .map_err(|_| PolarError::InvalidFormat("minecraft:air state missing from internal mapping"))
}

fn section_palette(
    internal_palette: &[InternalId],
    indices: Option<&[u32]>,
    air: InternalId,
) -> Palette {
    if internal_palette.len() == 1 {
        return Palette::single(internal_palette[0]);
    }

    let Some(indices) = indices else {
        return Palette::single(air);
    };

    if internal_palette.len() <= 256 {
        let bits_per_entry = bits_needed(internal_palette.len()).clamp(4, 8) as u8;
        let packed_data = pack_direct(indices.iter().copied(), bits_per_entry);
        Palette::paletted(bits_per_entry, internal_palette.to_vec(), packed_data)
    } else {
        let internal_data = indices
            .iter()
            .map(|idx| {
                let palette_idx = *idx as usize;
                internal_palette.get(palette_idx).copied().unwrap_or(air)
            })
            .collect();
        Palette::direct(internal_data)
    }
}

fn bits_needed(len: usize) -> u32 {
    if len <= 1 {
        1
    } else {
        (len as u32 - 1).ilog2() + 1
    }
}

fn unpack_palette_indices(
    packed: &[i64],
    palette_len: usize,
    output_len: usize,
) -> Result<Option<Vec<u32>>, PolarError> {
    if packed.is_empty() {
        return Ok(Some(vec![0; output_len]));
    }

    let bits_per_entry = bits_needed(palette_len) as usize;
    let ints_per_long = (64 / bits_per_entry).max(1);
    let mask = if bits_per_entry >= 64 {
        u64::MAX
    } else {
        (1u64 << bits_per_entry) - 1
    };

    let mut out = vec![0_u32; output_len];
    for (i, slot) in out.iter_mut().enumerate() {
        let long_index = i / ints_per_long;
        let sub_index = i % ints_per_long;

        let Some(word) = packed.get(long_index) else {
            return Err(PolarError::InvalidFormat(
                "palette data shorter than expected",
            ));
        };

        let value = ((*word as u64) >> (bits_per_entry * sub_index)) & mask;
        *slot = value as u32;
    }

    Ok(Some(out))
}

fn read_var_int(reader: &mut BinaryReader) -> Result<i32, PolarError> {
    Ok(reader.read::<VarInt>()?.inner())
}

fn read_bool(reader: &mut BinaryReader) -> Result<bool, PolarError> {
    Ok(reader.read::<u8>()? != 0)
}

fn read_string(reader: &mut BinaryReader) -> Result<String, PolarError> {
    let length = read_var_int(reader)?;
    if length < 0 {
        return Err(PolarError::InvalidFormat("negative string length"));
    }
    let length = length as usize;

    let mut bytes = vec![0u8; length];
    read_exact(reader, &mut bytes)?;

    Ok(String::from_utf8(bytes)
        .map_err(|_| PolarError::InvalidFormat("invalid utf-8 in string field"))?)
}

fn read_string_list(reader: &mut BinaryReader) -> Result<Vec<String>, PolarError> {
    let count = read_var_int(reader)?;
    if count < 0 {
        return Err(PolarError::InvalidFormat("negative list length"));
    }

    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..count {
        values.push(read_string(reader)?);
    }

    Ok(values)
}

fn read_optional_string(reader: &mut BinaryReader) -> Result<Option<String>, PolarError> {
    if read_bool(reader)? {
        Ok(Some(read_string(reader)?))
    } else {
        Ok(None)
    }
}

fn read_long_array(reader: &mut BinaryReader) -> Result<Vec<i64>, PolarError> {
    let count = read_var_int(reader)?;
    if count < 0 {
        return Err(PolarError::InvalidFormat("negative long array length"));
    }

    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..count {
        values.push(reader.read::<i64>()?);
    }

    Ok(values)
}

fn read_byte_array(reader: &mut BinaryReader) -> Result<Vec<i8>, PolarError> {
    let count = read_var_int(reader)?;
    if count < 0 {
        return Err(PolarError::InvalidFormat("negative byte array length"));
    }

    let mut values = Vec::with_capacity(count as usize);
    for _ in 0..count {
        values.push(reader.read::<i8>()?);
    }

    Ok(values)
}

fn read_fixed_light_data(reader: &mut BinaryReader) -> Result<Vec<i8>, PolarError> {
    let mut data = vec![0u8; LIGHT_SECTION_BYTES];
    read_exact(reader, &mut data)?;
    Ok(data.into_iter().map(|value| value as i8).collect())
}

fn read_light_content(reader: &mut BinaryReader) -> Result<LightContent, PolarError> {
    match reader.read::<i8>()? {
        0 => Ok(LightContent::Missing),
        1 => Ok(LightContent::Empty),
        2 => Ok(LightContent::Full),
        3 => Ok(LightContent::Present),
        _ => Err(PolarError::InvalidFormat("invalid light content flag")),
    }
}

fn empty_light() -> LightSection {
    vec![0; LIGHT_SECTION_BYTES]
}

fn full_light() -> LightSection {
    vec![-1; LIGHT_SECTION_BYTES]
}

fn light_to_section(content: LightContent, data: Option<Vec<i8>>) -> LightSection {
    match content {
        LightContent::Missing | LightContent::Empty => empty_light(),
        LightContent::Full => full_light(),
        LightContent::Present => data.unwrap_or_else(empty_light),
    }
}

fn read_heightmaps(reader: &mut BinaryReader) -> Result<Vec<Option<Vec<i32>>>, PolarError> {
    let mut heightmaps = vec![None; 32];
    let mask = reader.read::<i32>()?;

    for (index, slot) in heightmaps.iter_mut().enumerate() {
        if (mask & (1 << index)) == 0 {
            continue;
        }

        let packed = read_long_array(reader)?;
        if packed.is_empty() {
            *slot = Some(Vec::new());
            continue;
        }

        let bits_per_entry = (packed.len() * 64) / (16 * 16);
        if bits_per_entry == 0 {
            *slot = Some(vec![0; 16 * 16]);
            continue;
        }

        let ints_per_long = (64 / bits_per_entry).max(1);
        let mask = if bits_per_entry >= 64 {
            u64::MAX
        } else {
            (1u64 << bits_per_entry) - 1
        };

        let mut out = vec![0_i32; 16 * 16];
        for (i, value) in out.iter_mut().enumerate() {
            let long_index = i / ints_per_long;
            let sub_index = i % ints_per_long;
            let word = packed.get(long_index).copied().unwrap_or_default() as u64;
            *value = ((word >> (bits_per_entry * sub_index)) & mask) as i32;
        }

        *slot = Some(out);
    }

    Ok(heightmaps)
}

fn skip_nbt(reader: &mut BinaryReader, legacy_named_nbt: bool) -> Result<(), PolarError> {
    let tag_type = reader.read::<u8>()?;
    if tag_type == 0 {
        return Ok(());
    }

    if legacy_named_nbt {
        skip_nbt_name(reader)?;
    }

    skip_nbt_payload(reader, tag_type)
}

fn skip_nbt_name(reader: &mut BinaryReader) -> Result<(), PolarError> {
    let len = reader.read::<u16>()? as usize;
    skip_exact(reader, len)
}

fn skip_nbt_payload(reader: &mut BinaryReader, tag_type: u8) -> Result<(), PolarError> {
    match tag_type {
        1 => skip_exact(reader, 1),
        2 => skip_exact(reader, 2),
        3 => skip_exact(reader, 4),
        4 => skip_exact(reader, 8),
        5 => skip_exact(reader, 4),
        6 => skip_exact(reader, 8),
        7 => {
            let len = reader.read::<i32>()?;
            if len < 0 {
                return Err(PolarError::InvalidFormat("negative NBT byte array length"));
            }
            skip_exact(reader, len as usize)
        }
        8 => {
            let len = reader.read::<u16>()? as usize;
            skip_exact(reader, len)
        }
        9 => {
            let list_type = reader.read::<u8>()?;
            let len = reader.read::<i32>()?;
            if len < 0 {
                return Err(PolarError::InvalidFormat("negative NBT list length"));
            }

            for _ in 0..len {
                skip_nbt_payload(reader, list_type)?;
            }
            Ok(())
        }
        10 => {
            loop {
                let entry_type = reader.read::<u8>()?;
                if entry_type == 0 {
                    break;
                }
                skip_nbt_name(reader)?;
                skip_nbt_payload(reader, entry_type)?;
            }
            Ok(())
        }
        11 => {
            let len = reader.read::<i32>()?;
            if len < 0 {
                return Err(PolarError::InvalidFormat("negative NBT int array length"));
            }
            skip_exact(reader, len as usize * 4)
        }
        12 => {
            let len = reader.read::<i32>()?;
            if len < 0 {
                return Err(PolarError::InvalidFormat("negative NBT long array length"));
            }
            skip_exact(reader, len as usize * 8)
        }
        _ => Err(PolarError::InvalidFormat("unsupported NBT tag type")),
    }
}

fn skip_exact(reader: &mut BinaryReader, mut len: usize) -> Result<(), PolarError> {
    let mut scratch = [0u8; 1024];

    while len > 0 {
        let step = len.min(scratch.len());
        let read = reader.read_bytes(&mut scratch[..step])?;
        if read == 0 {
            return Err(PolarError::BinaryReader(BinaryReaderError::UnexpectedEof));
        }
        len -= read;
    }

    Ok(())
}

fn read_exact(reader: &mut BinaryReader, mut buf: &mut [u8]) -> Result<(), PolarError> {
    while !buf.is_empty() {
        let read = reader.read_bytes(buf)?;
        if read == 0 {
            return Err(PolarError::BinaryReader(BinaryReaderError::UnexpectedEof));
        }
        let (_, rest) = buf.split_at_mut(read);
        buf = rest;
    }

    Ok(())
}
