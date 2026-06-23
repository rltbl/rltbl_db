//! Serialization / deserialization implementations for rltbl_db.

use crate::{
    core::DbError,
    db_value::{DbRow, DbValue, JsonRow, JsonValue},
};
use serde::{
    Deserialize, Serialize,
    de::{self, Visitor},
    ser,
};
use serde_json::{json, value::Serializer as JsonValueSerializer};

/// Convert the given supported struct to a [DbRow]. For this to be successful, the struct
/// must be a struct of the form:
///
/// ```ignore
/// struct NormalStruct {
///   field1: type1, // or Option<type1>
///   field2: type2, // or Option<type2>
///   ...
/// }
/// ```
/// where `type1`, `type2` can either be a primitive or a complex type. In the case
/// of a complex type, the associated field will be serialized as a JSON string.
pub fn to_db_row<T>(value: &T) -> Result<DbRow, DbError>
where
    T: Serialize,
{
    // Serialize the given value.
    let mut serializer = DbRowSerializer::new();
    value.serialize(&mut serializer)?;

    // Collect the serialized keys and values and construct the DbRow that will be returned
    let keys = serializer.keys;
    let values = serializer.values;
    if keys.len() != values.len() {
        return Err(DbError::SerdeError(format!(
            "Keys and values have different lengths: \
             Keys: {keys:?} (length: {klen}), \
             Values: {values:?} (length: {vlen})",
            klen = keys.len(),
            vlen = values.len(),
        )));
    }
    let mut db_row = DbRow::new();
    for (i, key) in keys.iter().enumerate() {
        db_row.insert(key.to_string(), values[i].clone());
    }
    Ok(db_row)
}

/// Convert the given [DbRow] to a supported struct. For this to be successful,
/// the struct must be a "normal struct" of the form:
///
/// ```ignore
/// struct NormalStruct {
///   field1: type1, // or Option<type1>
///   field2: type2, // or Option<type2>
///   ...
/// }
/// ```
/// where `type1`, `type2` can either be a primitive or a complex type.
pub fn from_db_row<T>(db_row: &DbRow) -> Result<T, DbError>
where
    T: for<'de> Deserialize<'de>,
{
    let mut deserializer = DbRowDeserializer::from_db_row(db_row);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.keys.is_empty() && deserializer.values.is_empty() {
        Ok(t)
    } else {
        Err(DbError::SerdeError(format!(
            "Deserialization error: Leftover keys: {:?} and/or values: {:?}",
            deserializer.keys, deserializer.values
        )))
    }
}

////////////////////////////////////////////////////////////////////////////////
// Serialization implementations
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Default)]
struct DbRowSerializer {
    /// The keys of the serialized output [DbRow].
    keys: Vec<String>,
    /// The values of the serialized output [DbRow].
    values: Vec<DbValue>,
    /// When dealing with compound field types, this is used to keep track of the number of
    /// sub-fields of the given type. These are progressively added to `inner_value` until
    /// there are no further sub-fields to process.
    remaining_fields: usize,
    /// Represents the value of a compound type. Once the compound type has been fully
    /// serialized, it is cleared and added to `values`.
    inner_value: JsonValue,
}

impl DbRowSerializer {
    fn new() -> Self {
        DbRowSerializer::default()
    }
}

impl<'a> ser::Serializer for &'a mut DbRowSerializer {
    // The output type produced by this `DbRowSerializer` during successful
    // serialization.
    type Ok = ();

    // The error type when some error occurs during serialization.
    type Error = DbError;

    // Associated types for keeping track of additional state while serializing compound data
    // structures like sequences and maps. In this case no additional state is required beyond
    // what is already stored in the DbRowSerializer struct.
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    // Primitive types

    fn serialize_bool(self, value: bool) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_i8(self, value: i8) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_i16(self, value: i16) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_i32(self, value: i32) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_i64(self, value: i64) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_u8(self, value: u8) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_u16(self, value: u16) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_u32(self, value: u32) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_u64(self, value: u64) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_f64(self, value: f64) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_str(self, value: &str) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_char(self, value: char) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value.to_string()));
        Ok(())
    }

    // Option types

    fn serialize_none(self) -> Result<(), Self::Error> {
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    // Serializes an absent value (None)
    fn serialize_unit(self) -> Result<(), Self::Error> {
        self.values.push(DbValue::Null);
        Ok(())
    }

    // Compound types:

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        if self.keys.len() > self.values.len() {
            self.remaining_fields = len;
            // Start a new empty inner value which will be progressively filled in later:
            self.inner_value = json!([]);
            Ok(self)
        } else {
            Ok(self)
        }
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        if self.keys.len() > self.values.len() {
            self.remaining_fields =
                len.ok_or(DbError::SerdeError("No length given".to_string()))?;
            // Start a new empty inner value which will be progressively filled in later:
            self.inner_value = json!([]);
            Ok(self)
        } else {
            Ok(self)
        }
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(self)
    }

    fn serialize_struct(
        self,
        _name: &str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        if self.keys.len() > self.values.len() {
            self.remaining_fields = len;
            // Start a new empty inner value which will be progressively filled in later:
            self.inner_value = json!({});
            Ok(self)
        } else {
            self.serialize_map(Some(len))
        }
    }

    // A "unit" struct has no fields.
    fn serialize_unit_struct(self, name: &str) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(name));
        Ok(())
    }

    fn serialize_newtype_struct<T>(self, _name: &str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_tuple_struct(
        self,
        _name: &str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        if self.keys.len() > self.values.len() {
            self.remaining_fields = len;
            // Start a new empty inner value which will be progressively filled in later:
            self.inner_value = json!([]);
            Ok(self)
        } else {
            self.serialize_seq(Some(len))
        }
    }

    fn serialize_unit_variant(
        self,
        _name: &str,
        _variant_index: u32,
        variant: &str,
    ) -> Result<(), Self::Error> {
        // TODO: I don't like the extra double-quotes here,
        // but serde_json wants them for deserializing,
        // and I get a lifetime error when I try to add them
        // during the deserializing step.
        self.values.push(DbValue::from(format!(r#""{variant}""#)));
        Ok(())
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &str,
        _variant_index: u32,
        variant: &str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let json_serializer = JsonValueSerializer;
        let json_value = value
            .serialize(json_serializer)
            .map_err(|err| DbError::SerdeError(err.to_string()))?;
        let json_value = json!({variant.to_string(): json_value});
        self.values.push(DbValue::Json(json_value));
        Ok(())
    }

    fn serialize_tuple_variant(
        self,
        _name: &str,
        _variant_index: u32,
        variant: &str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        if self.keys.len() > self.values.len() {
            self.remaining_fields = len;
            // Start a new inner value with a single key (the tuple variant's name).
            self.inner_value = json!({variant.to_string(): []});
            Ok(self)
        } else {
            Ok(self)
        }
    }

    // Unsupported compound types

    fn serialize_struct_variant(
        self,
        _name: &str,
        _variant_index: u32,
        _variant: &str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing struct variant is not supported".to_string(),
        ));
    }

    // Although we don't support fields with type &[u8], bytes fields are supported as long
    // as they are owned, i.e., Vec<u8>, throush serialize_seq().
    fn serialize_bytes(self, _values: &[u8]) -> Result<(), Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing bytes is not supported".to_string(),
        ));
    }
}

impl<'a> ser::SerializeStruct for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        if self.remaining_fields > 0 {
            self.remaining_fields -= 1;
            let json_serializer = JsonValueSerializer;
            let json_value = value
                .serialize(json_serializer)
                .map_err(|err| DbError::SerdeError(err.to_string()))?;
            let inner = match self.inner_value.as_object_mut() {
                Some(inner) => inner,
                None => {
                    return Err(DbError::SerdeError(format!(
                        "Not a JSON Object: {}",
                        self.inner_value
                    )));
                }
            };
            inner.insert(key.to_string(), json_value);
        } else {
            self.keys.push(key.to_string());
            value.serialize(&mut **self)?;
        }
        Ok(())
    }

    fn end(self) -> Result<(), DbError> {
        if self.inner_value != JsonValue::Null {
            self.values.push(DbValue::Json(self.inner_value.clone()));
            self.inner_value = JsonValue::Null;
        }
        Ok(())
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        if self.remaining_fields > 0 {
            self.remaining_fields -= 1;
            let json_serializer = JsonValueSerializer;
            let json_value = value
                .serialize(json_serializer)
                .map_err(|err| DbError::SerdeError(err.to_string()))?;
            let inner = match self.inner_value.as_array_mut() {
                Some(inner) => inner,
                None => {
                    return Err(DbError::SerdeError(format!(
                        "Not a JSON Array: {}",
                        self.inner_value
                    )));
                }
            };
            inner.push(json_value);
        } else {
            value.serialize(&mut **self)?;
        }
        Ok(())
    }

    fn end(self) -> Result<(), DbError> {
        if self.inner_value != JsonValue::Null {
            self.values.push(DbValue::Json(self.inner_value.clone()));
            self.inner_value = JsonValue::Null;
        }
        Ok(())
    }
}

impl<'a> ser::SerializeTuple for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        if self.remaining_fields > 0 {
            self.remaining_fields -= 1;
            let json_serializer = JsonValueSerializer;
            let json_value = value
                .serialize(json_serializer)
                .map_err(|err| DbError::SerdeError(err.to_string()))?;
            let inner = match self.inner_value.as_array_mut() {
                Some(inner) => inner,
                None => {
                    return Err(DbError::SerdeError(format!(
                        "Not a JSON Array: {}",
                        self.inner_value
                    )));
                }
            };
            inner.push(json_value);
        } else {
            value.serialize(&mut **self)?;
        }
        Ok(())
    }

    fn end(self) -> Result<(), DbError> {
        if self.inner_value != JsonValue::Null {
            self.values.push(DbValue::Json(self.inner_value.clone()));
            self.inner_value = JsonValue::Null;
        }
        Ok(())
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        if self.remaining_fields > 0 {
            self.remaining_fields -= 1;
            let json_serializer = JsonValueSerializer;
            let json_value = value
                .serialize(json_serializer)
                .map_err(|err| DbError::SerdeError(err.to_string()))?;
            let inner = match self.inner_value.as_object_mut() {
                Some(inner) => inner,
                None => {
                    return Err(DbError::SerdeError(format!(
                        "Not a JSON object: {}",
                        self.inner_value
                    )));
                }
            };
            for (_key, json_array) in inner.into_iter() {
                let json_array_as_string = json_array.to_string();
                let json_array: &mut Vec<JsonValue> =
                    json_array
                        .as_array_mut()
                        .ok_or(DbError::SerdeError(format!(
                            "Not an array: {json_array_as_string}"
                        )))?;
                json_array.push(json_value.clone());
            }
        } else {
            value.serialize(&mut **self)?;
        }
        Ok(())
    }

    fn end(self) -> Result<(), DbError> {
        if self.inner_value != JsonValue::Null {
            self.values.push(DbValue::Json(self.inner_value.clone()));
            self.inner_value = JsonValue::Null;
        }
        Ok(())
    }
}

impl<'a> ser::SerializeSeq for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        if self.remaining_fields > 0 {
            self.remaining_fields -= 1;
            let json_serializer = JsonValueSerializer;
            let json_value = value
                .serialize(json_serializer)
                .map_err(|err| DbError::SerdeError(err.to_string()))?;
            let inner = match self.inner_value.as_array_mut() {
                Some(inner) => inner,
                None => {
                    return Err(DbError::SerdeError(format!(
                        "Not a JSON Array: {}",
                        self.inner_value
                    )));
                }
            };
            inner.push(json_value);
        } else {
            value.serialize(&mut **self)?;
        }
        Ok(())
    }

    fn end(self) -> Result<(), Self::Error> {
        if self.inner_value != JsonValue::Null {
            self.values.push(DbValue::Json(self.inner_value.clone()));
            self.inner_value = JsonValue::Null;
        }
        Ok(())
    }
}

// This might be called when serializing a JSON value that happens to be a map.
impl<'a> ser::SerializeMap for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        // If this is the first entry, create a new inner map:
        if self.inner_value == JsonValue::Null {
            self.inner_value = json!({
                "keys": [],
                "values": [],
            });
        }
        // Add the key to the keys list:
        match self
            .inner_value
            .get_mut("keys")
            .and_then(|k| k.as_array_mut())
        {
            Some(inner) => inner.push(json!(key)),
            None => {
                return Err(DbError::SerdeError(format!(
                    "Invalid inner value: {:?}",
                    self.inner_value
                )));
            }
        };
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        // Add the value to the values list:
        match self
            .inner_value
            .get_mut("values")
            .and_then(|v| v.as_array_mut())
        {
            Some(inner) => inner.push(json!(value)),
            None => {
                return Err(DbError::SerdeError(format!(
                    "Invalid inner value: {:?}",
                    self.inner_value
                )));
            }
        };
        Ok(())
    }

    fn end(self) -> Result<(), DbError> {
        // This is an inner version of the algorithm used in to_db_row().
        let empty_vec = vec![];
        let keys = {
            if self.inner_value == JsonValue::Null {
                &empty_vec
            } else {
                match self.inner_value.get("keys").and_then(|k| k.as_array()) {
                    Some(inner) => inner,
                    None => {
                        return Err(DbError::SerdeError(format!(
                            "Invalid inner value: {:?}",
                            self.inner_value
                        )));
                    }
                }
            }
        };
        let values = {
            if self.inner_value == JsonValue::Null {
                &empty_vec
            } else {
                match self.inner_value.get("values").and_then(|v| v.as_array()) {
                    Some(inner) => inner,
                    None => {
                        return Err(DbError::SerdeError(format!(
                            "Invalid inner value: {:?}",
                            self.inner_value
                        )));
                    }
                }
            }
        };
        if keys.len() != values.len() {
            return Err(DbError::SerdeError(format!(
                "Inner keys and values have different lengths: \
                 Keys: {keys:?} (length: {klen}), \
                 Values: {values:?} (length: {vlen})",
                klen = keys.len(),
                vlen = values.len(),
            )));
        }
        let mut json_row = JsonRow::new();
        for (i, key) in keys.iter().enumerate() {
            let key = key
                .as_str()
                .ok_or(DbError::SerdeError(format!("Not a string: {key}")))?
                .to_string();
            json_row.insert(key, values[i].clone());
        }
        self.values.push(DbValue::Json(json!(json_row)));
        self.inner_value = JsonValue::Null;
        Ok(())
    }
}

// Unsupported implementations

impl<'a> ser::SerializeStructVariant for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeStructVariant::serialize_field() is not supported for DbRowSerializer"
                .to_string(),
        ));
    }

    fn end(self) -> Result<(), DbError> {
        return Err(DbError::SerdeError(
            "SerializeStructVariant::end() is not supported for DbRowSerializer".to_string(),
        ));
    }
}

////////////////////////////////////////////////////////////////////////////////
// Deserialization implementations
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct DbRowDeserializer<'de> {
    first: bool,
    /// The keys of the input [DbRow].
    keys: Vec<&'de str>,
    /// The values of the input [DbRow].
    values: Vec<&'de DbValue>,
}

impl<'de> DbRowDeserializer<'de> {
    pub(crate) fn from_db_row(input: &'de DbRow) -> Self {
        DbRowDeserializer {
            first: true,
            keys: input.map.keys().map(|s| s.as_str()).collect::<Vec<_>>(),
            values: input.map.values().collect::<Vec<_>>(),
        }
    }

    fn pop_key(&mut self) -> Option<&'de str> {
        self.keys.pop()
    }

    fn pop_value(&mut self) -> Result<&'de DbValue, DbError> {
        self.values
            .pop()
            .ok_or(DbError::SerdeError("No values to pop".to_string()))
    }

    fn last_value(&self) -> Result<&DbValue, DbError> {
        self.values
            .last()
            .ok_or(DbError::SerdeError("No values".to_string()))
            .copied()
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut DbRowDeserializer<'de> {
    type Error = DbError;

    // Primitives:

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_bool()
                    .ok_or(DbError::SerdeError(format!("Not a boolean: {value}")))?;
                visitor.visit_bool(value)
            }
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_i8()
                    .ok_or(DbError::SerdeError(format!("Not an i8: {value}")))?;
                visitor.visit_i8(value)
            }
        }
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_i16()
                    .ok_or(DbError::SerdeError(format!("Not an i16: {value}")))?;
                visitor.visit_i16(value)
            }
        }
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_i32()
                    .ok_or(DbError::SerdeError(format!("Not an i32: {value}")))?;
                visitor.visit_i32(value)
            }
        }
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_i64()
                    .ok_or(DbError::SerdeError(format!("Not an i64: {value}")))?;
                visitor.visit_i64(value)
            }
        }
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_u8()
                    .ok_or(DbError::SerdeError(format!("Not a u8: {value}")))?;
                visitor.visit_u8(value)
            }
        }
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_u16()
                    .ok_or(DbError::SerdeError(format!("Not a u16: {value}")))?;
                visitor.visit_u16(value)
            }
        }
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_u32()
                    .ok_or(DbError::SerdeError(format!("Not a u32: {value}")))?;
                visitor.visit_u32(value)
            }
        }
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_u64()
                    .ok_or(DbError::SerdeError(format!("Not a u64: {value}")))?;
                visitor.visit_u64(value)
            }
        }
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_f32()
                    .ok_or(DbError::SerdeError(format!("Not an f32: {value}")))?;
                visitor.visit_f32(value)
            }
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_f64()
                    .ok_or(DbError::SerdeError(format!("Not an f64: {value}")))?;
                visitor.visit_f64(value)
            }
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_str()
                    .ok_or(DbError::SerdeError(format!("Not a string: {value}")))?;
                visitor.visit_borrowed_str(value)
            }
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_str()
                    .ok_or(DbError::SerdeError(format!("Not a char: {value}")))?;
                visitor.visit_borrowed_str(value)
            }
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value()?;
                let value = value
                    .as_str()
                    .ok_or(DbError::SerdeError(format!("Not a str: {value}")))?;
                visitor.visit_borrowed_str(value)
            }
        }
    }

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => match self.pop_value()? {
                DbValue::Null => unreachable!(),
                DbValue::Text(value) => visitor.visit_borrowed_str(&value),
                DbValue::Boolean(value) => visitor.visit_bool(*value),
                DbValue::SmallInteger(value) => visitor.visit_i16(*value),
                DbValue::Integer(value) => visitor.visit_i32(*value),
                DbValue::BigInteger(value) => visitor.visit_i64(*value),
                DbValue::Real(value) => visitor.visit_f32(*value),
                DbValue::BigReal(value) => visitor.visit_f64(*value),
                // Rust's serializer seems to always serialize Decimals as text, so we don't
                // need to support this match branch (yet):
                DbValue::Numeric(_) => Err(DbError::SerdeError(
                    "Deserializing Decimal values is not yet supported".to_string(),
                )),
                DbValue::Json(value) => match value {
                    JsonValue::Array(_) => value.deserialize_seq(visitor).map_err(|err| {
                        DbError::SerdeError(format!("Error deserializing array: '{err}'."))
                    }),
                    JsonValue::Object(_) => value.deserialize_map(visitor).map_err(|err| {
                        DbError::SerdeError(format!("Error deserializing object: '{err}'."))
                    }),
                    _ => {
                        return Err(DbError::SerdeError(format!(
                            "Invalid JSON value: {value:?}"
                        )));
                    }
                },
                DbValue::Other(type_name, bytes, string_opt) => Err(DbError::SerdeError(format!(
                    "Deserialization not supported for \
                     DbValue::Other({type_name}, {bytes:?}, {string_opt:?})"
                ))),
            },
        }
    }

    // Options

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.last_value()? {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => visitor.visit_some(self),
        }
    }

    // Deserializes to an absent value (i.e., a None).
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        let value = self.pop_value()?;
        if *value != DbValue::Null {
            return Err(DbError::SerdeError("Expected NULL".to_string()));
        }
        visitor.visit_unit()
    }

    // Compound types

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        if self.first {
            self.first = false;
            self.deserialize_map(visitor)
        } else {
            let value = self.pop_value()?;
            match value {
                DbValue::Text(value) | DbValue::Json(JsonValue::String(value)) => {
                    serde_json::Deserializer::from_str(value)
                        .deserialize_struct(name, fields, visitor)
                        .map_err(|err| DbError::SerdeError(err.to_string()))
                }
                DbValue::Json(value) => match value {
                    JsonValue::Array(_) => value.deserialize_map(visitor).map_err(|err| {
                        DbError::SerdeError(format!("Error deserializing array: '{err}'."))
                    }),
                    JsonValue::Object(_) => value.deserialize_map(visitor).map_err(|err| {
                        DbError::SerdeError(format!("Error deserializing object: '{err}'."))
                    }),
                    _ => {
                        return Err(DbError::SerdeError(format!(
                            "Invalid JSON value: {value:?}"
                        )));
                    }
                },
                _ => return Err(DbError::SerdeError(format!("Invalid DB value: {value:?}"))),
            }
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        let key = self
            .pop_key()
            .ok_or(DbError::SerdeError("No more keys".to_string()))?;
        visitor.visit_borrowed_str(key)
    }

    // A "unit" struct has no fields.
    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        self.pop_value()?;
        visitor.visit_unit()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        let value = self.pop_value()?;
        match value {
            DbValue::Text(value) | DbValue::Json(JsonValue::String(value)) => {
                serde_json::Deserializer::from_str(value)
                    .deserialize_enum(name, variants, visitor)
                    .map_err(|err| DbError::SerdeError(err.to_string()))
            }
            DbValue::Json(value) => {
                match value {
                    JsonValue::Array(_) => {
                        value
                            .deserialize_enum(name, variants, visitor)
                            .map_err(|err| {
                                DbError::SerdeError(format!("Error deserializing array: '{err}'."))
                            })
                    }
                    JsonValue::Object(_) => value
                        .deserialize_enum(name, variants, visitor)
                        .map_err(|err| {
                            DbError::SerdeError(format!("Error deserializing object: '{err}'."))
                        }),
                    _ => {
                        return Err(DbError::SerdeError(format!(
                            "Invalid JSON value: {value:?}"
                        )));
                    }
                }
            }
            _ => return Err(DbError::SerdeError(format!("Invalid DB value: {value:?}"))),
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        let value = self.pop_value()?;
        match value {
            DbValue::Text(value) | DbValue::Json(JsonValue::String(value)) => {
                serde_json::Deserializer::from_str(value)
                    .deserialize_seq(visitor)
                    .map_err(|err| DbError::SerdeError(err.to_string()))
            }
            DbValue::Json(value) => match value {
                JsonValue::Array(_) => value.deserialize_seq(visitor).map_err(|err| {
                    DbError::SerdeError(format!("Error deserializing array: '{err}'."))
                }),
                JsonValue::Object(_) => value.deserialize_seq(visitor).map_err(|err| {
                    DbError::SerdeError(format!("Error deserializing object: '{err}'."))
                }),
                _ => {
                    return Err(DbError::SerdeError(format!(
                        "Invalid JSON value: {value:?}"
                    )));
                }
            },
            _ => return Err(DbError::SerdeError(format!("Invalid DB value: {value:?}"))),
        }
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        let value = self.pop_value()?;
        match value {
            DbValue::Text(value) | DbValue::Json(JsonValue::String(value)) => {
                serde_json::Deserializer::from_str(value)
                    .deserialize_seq(visitor)
                    .map_err(|err| DbError::SerdeError(err.to_string()))
            }
            DbValue::Json(value) => match value {
                JsonValue::Array(_) => value.deserialize_seq(visitor).map_err(|err| {
                    DbError::SerdeError(format!("Error deserializing array: '{err}'."))
                }),
                JsonValue::Object(_) => value.deserialize_seq(visitor).map_err(|err| {
                    DbError::SerdeError(format!("Error deserializing object: '{err}'."))
                }),
                _ => {
                    return Err(DbError::SerdeError(format!(
                        "Invalid JSON value: {value:?}"
                    )));
                }
            },
            _ => return Err(DbError::SerdeError(format!("Invalid DB value: {value:?}"))),
        }
    }

    // Unsupported types

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'bytes' is not supported".to_string(),
        ));
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'byte_buf' is not supported".to_string(),
        ));
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'ignored_any' is not supported".to_string(),
        ));
    }
}

impl<'de> de::MapAccess<'de> for DbRowDeserializer<'de> {
    type Error = DbError;

    fn next_key_seed<S>(&mut self, seed: S) -> Result<Option<S::Value>, Self::Error>
    where
        S: de::DeserializeSeed<'de>,
    {
        if self.keys.len() == 0 {
            return Ok(None);
        }
        seed.deserialize(&mut *self).map(Some)
    }

    fn next_value_seed<S>(&mut self, seed: S) -> Result<S::Value, Self::Error>
    where
        S: de::DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db_row,
        db_value::{DbValue, JsonValue},
    };
    use rust_decimal::{Decimal, dec};
    use serde::Deserialize;

    #[test]
    fn test_serde_default() {
        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone, Default)]
        #[serde(default)]
        struct TestStruct {
            foo: String,
            bar: String,
        }

        let expected_struct = TestStruct {
            foo: "FOO".to_string(),
            bar: "".to_string(),
        };
        let db_row = db_row! {
            "foo" => "FOO",
            // "bar" => "",
        };
        assert_eq!(Ok(expected_struct), from_db_row(&db_row));
    }

    #[test]
    fn test_serde() {
        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct UnitStruct;

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct SimpleStruct {
            foo: u64,
        }

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct NormalStruct {
            foo: String,
            bar: u64,
            list: Vec<i16>,
            tuple: (u64, String),
            json: JsonValue,
        }

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct NewTypeStruct(i64);

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct TupleStruct(i64, i64);

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        enum Enumeration {
            UnitVariant,
            NewTypeVariant(f32),
            StructVariant(SimpleStruct),
            TupleVariant(i64, i64),
        }

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct NestedStruct {
            foo: Enumeration,
            bar: NormalStruct,
            foo_list: Vec<Enumeration>,
            bar_list: Vec<NormalStruct>,
            bar_tuple: (u32, i32, String),
        }

        // Serializing and deserializing an arbitrary struct to a DbRow:
        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct TestStruct {
            // json
            json_simple_1: JsonValue,
            json_simple_2: JsonValue,
            json_simple_3: JsonValue,
            json_simple_4: JsonValue,
            json_complex: JsonValue,
            json_opt_none: Option<JsonValue>,
            json_opt_some: Option<JsonValue>,

            // bool
            boolean: bool,
            boolean_opt_none: Option<bool>,
            boolean_opt_some: Option<bool>,
            //
            // i8
            tinyint: i8,
            tinyint_opt_none: Option<i8>,
            tinyint_opt_some: Option<i8>,
            //
            // i16
            smallint: i16,
            smallint_opt_none: Option<i16>,
            smallint_opt_some: Option<i16>,
            //
            // i32
            mediumint: i32,
            mediumint_opt_none: Option<i32>,
            mediumint_opt_some: Option<i32>,
            //
            // i64
            bigint: i64,
            bigint_opt_none: Option<i64>,
            bigint_opt_some: Option<i64>,
            //
            // u8
            tiny_unsigned: u8,
            tiny_unsigned_opt_none: Option<u8>,
            tiny_unsigned_opt_some: Option<u8>,
            //
            // u16
            small_unsigned: u16,
            small_unsigned_opt_none: Option<u16>,
            small_unsigned_opt_some: Option<u16>,
            //
            // u32
            medium_unsigned: u32,
            medium_unsigned_opt_none: Option<u32>,
            medium_unsigned_opt_some: Option<u32>,
            //
            // u64
            big_unsigned: u64,
            big_unsigned_opt_none: Option<u64>,
            big_unsigned_opt_some: Option<u64>,
            //
            // f32
            smallfloat: f32,
            smallfloat_opt_none: Option<f32>,
            smallfloat_opt_some: Option<f32>,
            //
            // f64
            bigfloat: f64,
            bigfloat_opt_none: Option<f64>,
            bigfloat_opt_some: Option<f64>,
            //
            // String
            text: String,
            text_opt_none: Option<String>,
            text_opt_some: Option<String>,
            //
            // Note that rust's serializer serializes Decimal values to text (see also below).
            biggerfloat: Decimal,
            biggerfloat_opt_none: Option<Decimal>,
            biggerfloat_opt_some: Option<Decimal>,
            //
            // A vector of u8 (bytes)
            sequence: Vec<u8>,
            sequence_opt_none: Option<Vec<u8>>,
            sequence_opt_some: Option<Vec<u8>>,
            //
            // Enum
            enumeration: Enumeration,
            enumeration_opt_none: Option<Enumeration>,
            enumeration_opt_some: Option<Enumeration>,
            //
            // Struct variant
            struct_variant: Enumeration,
            struct_variant_opt_none: Option<Enumeration>,
            struct_variant_opt_some: Option<Enumeration>,
            //
            // Tuple variant
            tuple_variant: Enumeration,
            tuple_variant_opt_none: Option<Enumeration>,
            tuple_variant_opt_some: Option<Enumeration>,
            //
            // Unit struct
            unit_struct: UnitStruct,
            unit_struct_opt_none: Option<UnitStruct>,
            unit_struct_opt_some: Option<UnitStruct>,
            //
            // Struct
            structure: NormalStruct,
            structure_opt_none: Option<NormalStruct>,
            structure_opt_some: Option<NormalStruct>,
            //
            // Newtype struct
            newtype_struct: NewTypeStruct,
            newtype_struct_opt_none: Option<NewTypeStruct>,
            newtype_struct_opt_some: Option<NewTypeStruct>,
            //
            // Tuple
            tuple: (u32, String),
            tuple_opt_none: Option<(u32, String)>,
            tuple_opt_some: Option<(u32, String)>,
            //
            // Tuple struct
            tuple_struct: TupleStruct,
            tuple_struct_opt_none: Option<TupleStruct>,
            tuple_struct_opt_some: Option<TupleStruct>,
            //
            // Nested struct
            nested_struct: NestedStruct,
            nested_struct_opt_none: Option<NestedStruct>,
            nested_struct_opt_some: Option<NestedStruct>,
        }

        let expected_struct = TestStruct {
            json_simple_1: json!(1724),
            json_simple_2: JsonValue::Null,
            json_simple_3: JsonValue::Bool(true),
            json_simple_4: JsonValue::String("foo".into()),
            json_complex: json!({"alpha": 10}),
            json_opt_none: None,
            json_opt_some: Some(json!({})),

            boolean: true,
            boolean_opt_none: None,
            boolean_opt_some: Some(true),

            tinyint: 1,
            tinyint_opt_none: None,
            tinyint_opt_some: Some(1),

            smallint: 1,
            smallint_opt_none: None,
            smallint_opt_some: Some(1),

            mediumint: 1,
            mediumint_opt_none: None,
            mediumint_opt_some: Some(1),

            bigint: 1,
            bigint_opt_none: None,
            bigint_opt_some: Some(1),

            tiny_unsigned: 1,
            tiny_unsigned_opt_none: None,
            tiny_unsigned_opt_some: Some(1),

            small_unsigned: 1,
            small_unsigned_opt_none: None,
            small_unsigned_opt_some: Some(1),

            medium_unsigned: 1,
            medium_unsigned_opt_none: None,
            medium_unsigned_opt_some: Some(1),

            big_unsigned: 1,
            big_unsigned_opt_none: None,
            big_unsigned_opt_some: Some(1),

            smallfloat: 1_f32,
            smallfloat_opt_none: None,
            smallfloat_opt_some: Some(1_f32),

            bigfloat: 1_f64,
            bigfloat_opt_none: None,
            bigfloat_opt_some: Some(1_f64),

            text: 1.to_string(),
            text_opt_none: None,
            text_opt_some: Some(1.to_string()),

            biggerfloat: dec!(1),
            biggerfloat_opt_none: None,
            biggerfloat_opt_some: Some(dec!(1)),

            // Compound types:
            sequence: vec![0, 1, 2],
            sequence_opt_none: None,
            sequence_opt_some: Some(vec![0, 1, 2]),

            enumeration: Enumeration::NewTypeVariant(1.0),
            enumeration_opt_none: None,
            enumeration_opt_some: Some(Enumeration::UnitVariant),

            struct_variant: Enumeration::StructVariant(SimpleStruct { foo: 1 }),
            struct_variant_opt_none: None,
            struct_variant_opt_some: Some(Enumeration::StructVariant(SimpleStruct { foo: 1 })),

            tuple_variant: Enumeration::TupleVariant(1, 1),
            tuple_variant_opt_none: None,
            tuple_variant_opt_some: Some(Enumeration::TupleVariant(1, 1)),

            unit_struct: UnitStruct,
            unit_struct_opt_none: None,
            unit_struct_opt_some: Some(UnitStruct),

            structure: NormalStruct {
                foo: String::from("bar"),
                bar: 1,
                list: vec![1, 2, 3],
                tuple: (1, String::from("bar")),
                json: json!([13, 14.0]),
            },
            structure_opt_none: None,
            structure_opt_some: Some(NormalStruct {
                foo: String::from("bar"),
                bar: 1,
                list: vec![1, 2, 3],
                tuple: (1, String::from("bar")),
                json: json!({"foo": 13.2, "bar": "yes"}),
            }),

            newtype_struct: NewTypeStruct(1),
            newtype_struct_opt_none: None,
            newtype_struct_opt_some: Some(NewTypeStruct(1)),

            tuple: (1, String::from("bar")),
            tuple_opt_none: None,
            tuple_opt_some: Some((1, String::from("bar"))),

            tuple_struct: TupleStruct(111, 111),
            tuple_struct_opt_none: None,
            tuple_struct_opt_some: Some(TupleStruct(111, 111)),

            nested_struct: NestedStruct {
                foo: Enumeration::NewTypeVariant(1.0),
                bar: NormalStruct {
                    foo: String::from("bar"),
                    bar: 1,
                    list: vec![1, 2, 3],
                    tuple: (1, String::from("bar")),
                    json: json!([]),
                },
                foo_list: vec![Enumeration::NewTypeVariant(1.0)],
                bar_list: vec![NormalStruct {
                    foo: String::from("bar"),
                    bar: 1,
                    list: vec![1, 2, 3],
                    tuple: (1, String::from("bar")),
                    json: json!([JsonValue::Null, json!({})]),
                }],
                bar_tuple: (1, 1, String::from("bar")),
            },
            nested_struct_opt_none: None,
            nested_struct_opt_some: Some(NestedStruct {
                foo: Enumeration::NewTypeVariant(1.0),
                bar: NormalStruct {
                    foo: String::from("bar"),
                    bar: 1,
                    list: vec![1, 2, 3],
                    tuple: (1, String::from("bar")),
                    json: JsonValue::Null,
                },
                foo_list: vec![Enumeration::NewTypeVariant(1.0)],
                bar_list: vec![NormalStruct {
                    foo: String::from("bar"),
                    bar: 1,
                    list: vec![1, 2, 3],
                    tuple: (1, String::from("bar")),
                    json: json!("Semmelweis"),
                }],
                bar_tuple: (1, 1, String::from("bar")),
            }),
        };

        let expected_db_row = db_row! {
            "json_simple_1" => json!(1724),
            "json_simple_2" => JsonValue::Null,
            "json_simple_3" => JsonValue::Bool(true),
            "json_simple_4" => JsonValue::String("foo".into()),
            "json_complex" => json!({"alpha": 10}),
            "json_opt_none" => DbValue::Null,
            "json_opt_some" => json!({}),

            "boolean" => true,
            "boolean_opt_none" => DbValue::Null,
            "boolean_opt_some" => true,

            "tinyint" => 1_i16,
            "tinyint_opt_none" => DbValue::Null,
            "tinyint_opt_some" => 1_i16,

            "smallint" => 1_i16,
            "smallint_opt_none" => DbValue::Null,
            "smallint_opt_some" => 1_i16,

            "mediumint" => 1_i32,
            "mediumint_opt_none" => DbValue::Null,
            "mediumint_opt_some" => 1_i32,

            "bigint" => 1_i64,
            "bigint_opt_none" => DbValue::Null,
            "bigint_opt_some" => 1_i64,

            "tiny_unsigned" => 1_i16,
            "tiny_unsigned_opt_none" => DbValue::Null,
            "tiny_unsigned_opt_some" => 1_i16,

            "small_unsigned" => 1_i32,
            "small_unsigned_opt_none" => DbValue::Null,
            "small_unsigned_opt_some" => 1_i32,

            "medium_unsigned" => 1_u64,
            "medium_unsigned_opt_none" => DbValue::Null,
            "medium_unsigned_opt_some" => 1_u64,

            "big_unsigned" => 1_u64,
            "big_unsigned_opt_none" => DbValue::Null,
            "big_unsigned_opt_some" => 1_u64,

            "smallfloat" => 1_f32,
            "smallfloat_opt_none" => DbValue::Null,
            "smallfloat_opt_some" => 1_f32,

            "bigfloat" => 1_f64,
            "bigfloat_opt_none" => DbValue::Null,
            "bigfloat_opt_some" => 1_f64,

            "text" => "1",
            "text_opt_none" => DbValue::Null,
            "text_opt_some" => "1",

            // Serde interprets Decimals as text:
            "biggerfloat" => "1",
            "biggerfloat_opt_none" => DbValue::Null,
            "biggerfloat_opt_some" => "1",

            // Complex types:

            "sequence" => json!([0, 1, 2]),
            "sequence_opt_none" => DbValue::Null,
            "sequence_opt_some" => json!([0, 1, 2]),

            "enumeration" => json!({"NewTypeVariant": 1.0}),
            "enumeration_opt_none" => DbValue::Null,
            "enumeration_opt_some" => "\"UnitVariant\"",

            "struct_variant" => json!({"StructVariant":{"foo": 1}}),
            "struct_variant_opt_none" => DbValue::Null,
            "struct_variant_opt_some" => json!({"StructVariant":{"foo": 1}}),

            "tuple_variant" => json!({"TupleVariant":[1,1]}),
            "tuple_variant_opt_none" => DbValue::Null,
            "tuple_variant_opt_some" => json!({"TupleVariant":[1, 1]}),

            "unit_struct" => "UnitStruct",
            "unit_struct_opt_none" => DbValue::Null,
            "unit_struct_opt_some" => "UnitStruct",

            "structure" => json!({
                "foo": "bar",
                "bar": 1,
                "list": [1, 2, 3],
                "tuple": [1,"bar"],
                "json": [13, 14.0]
            }),
            "structure_opt_none" => DbValue::Null,
            "structure_opt_some" => json!({
                "foo": "bar",
                "bar": 1,
                "list": [1, 2, 3],
                "tuple": [1, "bar"],
                "json": {"foo": 13.2, "bar": "yes"}
            }),

            "newtype_struct" => 1_i64,
            "newtype_struct_opt_none" => DbValue::Null,
            "newtype_struct_opt_some" => 1_i64,

            "tuple" => json!([1,"bar"]),
            "tuple_opt_none" => DbValue::Null,
            "tuple_opt_some" => json!([1,"bar"]),

            "tuple_struct" => json!([111,111]),
            "tuple_struct_opt_none" => DbValue::Null,
            "tuple_struct_opt_some" => json!([111,111]),

            "nested_struct" => json!(
                {"foo": {"NewTypeVariant":1.0},
                 "bar": {
                     "foo": "bar",
                     "bar": 1,
                     "list": [1, 2, 3],
                     "tuple": [1, "bar"],
                     "json": []
                 },
                 "foo_list":[{"NewTypeVariant": 1.0}],
                 "bar_list":[{
                     "foo": "bar",
                     "bar": 1,
                     "list": [1, 2, 3],
                     "tuple": [1, "bar"],
                     "json": [JsonValue::Null, json!({})]
                 }],
                 "bar_tuple": [1, 1, "bar"]
                }
            ),
            "nested_struct_opt_none" => DbValue::Null,
            "nested_struct_opt_some" => json!(
                {"foo": {"NewTypeVariant": 1.0},
                 "bar": {
                     "foo": "bar",
                     "bar": 1,
                     "list": [1, 2, 3],
                     "tuple": [1, "bar"],
                     "json": JsonValue::Null
                 },
                 "foo_list":[{"NewTypeVariant": 1.0}],
                 "bar_list":[{
                     "foo": "bar",
                     "bar": 1,
                     "list": [1, 2, 3],
                     "tuple": [1, "bar"],
                     "json": "Semmelweis"
                 }],
                 "bar_tuple": [1, 1, "bar"]
                }
            ),
        };
        assert_eq!(
            expected_db_row,
            to_db_row(&expected_struct).unwrap(),
            "test serialize"
        );
        assert_eq!(
            expected_struct,
            from_db_row(&expected_db_row).unwrap(),
            "test deserialize"
        );
    }
}
