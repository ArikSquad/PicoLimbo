use crate::block_entities::generic::GenericBlockEntity;
use crate::block_entities::sign::SignBlockEntity;
use minecraft_protocol::prelude::{Coordinates, ProtocolVersion};
use pico_nbt::prelude::Nbt;
use std::fmt::Display;

#[derive(Clone)]
pub enum BlockEntityType {
    Sign,
    HangingSign,
    Generic(String),
}

impl Display for BlockEntityType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            BlockEntityType::Sign => "minecraft:sign".to_string(),
            BlockEntityType::HangingSign => "minecraft:hanging_sign".to_string(),
            BlockEntityType::Generic(type_id) => type_id.clone(),
        };
        write!(f, "{str}")
    }
}

impl From<&str> for BlockEntityType {
    fn from(type_id: &str) -> Self {
        match type_id {
            "sign" | "minecraft:sign" => BlockEntityType::Sign,
            "hanging_sign" | "minecraft:hanging_sign" => BlockEntityType::HangingSign,
            other => BlockEntityType::Generic(other.to_string()),
        }
    }
}

#[derive(Clone)]
pub struct BlockEntity {
    pub position: Coordinates,
    pub block_entity_type: BlockEntityType,
    pub block_entity_data: BlockEntityData,
}

impl BlockEntity {
    pub fn from_nbt(entity_nbt: &Nbt) -> Option<Self> {
        let coordinates = Self::extract_coordinates(entity_nbt);
        let id = entity_nbt.find_tag("Id").and_then(|nbt| nbt.get_string());

        if let Some(id_tag) = id.as_ref()
            && let Some(position) = coordinates
        {
            let block_entity_type = BlockEntityType::from(id_tag.as_str());
            let block_entity_data = BlockEntityData::from_nbt(id_tag.clone(), entity_nbt);
            Some(Self {
                position,
                block_entity_data,
                block_entity_type,
            })
        } else {
            None
        }
    }

    pub fn to_nbt(&self, protocol_version: ProtocolVersion) -> Nbt {
        self.block_entity_data.to_nbt(protocol_version)
    }

    pub fn get_block_entity_type(&self) -> &BlockEntityType {
        &self.block_entity_type
    }

    pub fn get_position(&self) -> Coordinates {
        self.position
    }

    fn extract_coordinates(entity_nbt: &Nbt) -> Option<Coordinates> {
        if let Some(explicit_xyz) = Self::extract_explicit_xyz(entity_nbt) {
            return Some(explicit_xyz);
        }

        let pos_tag = entity_nbt.find_tag("Pos")?;

        if let Some(pos_array) = pos_tag.get_int_array()
            && pos_array.len() >= 3
        {
            let x = pos_array[0];
            let mut y = pos_array[1];
            let z = pos_array[2];

            if y.abs() > 0xFFFF {
                let decoded_y = y >> 20;
                if decoded_y.abs() <= 4096 {
                    y = decoded_y;
                }
            }

            return Some(Coordinates::new(x, y, z));
        }

        if let Nbt::Long { value, .. } = pos_tag {
            let packed = *value;
            return Some(Self::decode_packed_block_pos(packed));
        }

        None
    }

    fn extract_explicit_xyz(entity_nbt: &Nbt) -> Option<Coordinates> {
        let x = entity_nbt
            .find_tag("x")
            .and_then(|tag| tag.get_int())
            .or_else(|| entity_nbt.find_tag("X").and_then(|tag| tag.get_int()));
        let y = entity_nbt
            .find_tag("y")
            .and_then(|tag| tag.get_int())
            .or_else(|| entity_nbt.find_tag("Y").and_then(|tag| tag.get_int()));
        let z = entity_nbt
            .find_tag("z")
            .and_then(|tag| tag.get_int())
            .or_else(|| entity_nbt.find_tag("Z").and_then(|tag| tag.get_int()));

        match (x, y, z) {
            (Some(x), Some(y), Some(z)) => Some(Coordinates::new(x, y, z)),
            _ => None,
        }
    }

    fn decode_packed_block_pos(value: i64) -> Coordinates {
        let x = ((value >> 38) & 0x3FFFFFF) as i32;
        let y = (value & 0xFFF) as i32;
        let z = ((value >> 12) & 0x3FFFFFF) as i32;

        let x = Self::sign_extend(x, 26);
        let y = Self::sign_extend(y, 12);
        let z = Self::sign_extend(z, 26);

        Coordinates::new(x, y, z)
    }

    fn sign_extend(value: i32, bits: u8) -> i32 {
        let shift = i32::BITS as u8 - bits;
        (value << shift) >> shift
    }
}

#[derive(Clone)]
pub enum BlockEntityData {
    Sign(Box<SignBlockEntity>),
    Generic { entity: GenericBlockEntity },
}

impl BlockEntityData {
    fn from_nbt(id_tag: String, entity_nbt: &Nbt) -> Self {
        match id_tag.as_str() {
            "minecraft:sign" | "minecraft:hanging_sign" | "sign" | "hanging_sign" => {
                Self::Sign(Box::new(SignBlockEntity::from_nbt(entity_nbt)))
            }

            _ => {
                Self::Generic {
                    entity: GenericBlockEntity::from_nbt(entity_nbt),
                }
            }
        }
    }

    fn to_nbt(&self, protocol_version: ProtocolVersion) -> Nbt {
        match self {
            BlockEntityData::Sign(entity) => entity.to_nbt(protocol_version),
            BlockEntityData::Generic { entity } => entity.to_nbt(),
        }
    }
}
