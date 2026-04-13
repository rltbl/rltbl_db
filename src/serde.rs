//! Serialization / deserialization implementations for rltbl_db.

use crate::{
    core::DbError,
    db_value::{DbRow, DbValue, JsonValue},
};
use serde::{
    Deserialize, Serialize,
    de::{self, Visitor},
    ser,
};
use serde_json::{json, value::Serializer as JsonValueSerializer};
use tracing::trace;

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
/// of a complex type, the associated field will be serialized as JSON.
pub fn to_db_row<T>(value: &T) -> Result<DbRow, DbError>
where
    T: Serialize,
{
    trace!("to_db_row(...)");

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
    trace!("from_db_row({db_row:?})");

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
    /// When dealing with complex field types, this is used to skip the sub-fields
    /// of the given type, since complex types are serialized as JSON.
    skip: usize,
    /// Represents the value of a complex type.
    inner_value: JsonValue,
}

impl DbRowSerializer {
    fn new() -> Self {
        trace!("DbRowSerializer::new()");
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
        trace!("DbRowSerializer::serialize_bool({self:#?}, {value})");
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_i8(self, value: i8) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_i8({self:#?}, {value})");
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_i16(self, value: i16) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_i16({self:#?}, {value})");
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_i32(self, value: i32) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_i32({self:#?}, {value})");
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_i64(self, value: i64) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_i64({self:#?}, {value})");
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_u8(self, value: u8) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_u8({self:#?}, {value})");
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_u16(self, value: u16) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_u16({self:#?}, {value})");
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_u32(self, value: u32) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_u32({self:#?}, {value})");
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_u64(self, value: u64) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_u64({self:#?}, {value})");
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_f32({self:#?}, {value})");
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_f64(self, value: f64) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_f64({self:#?}, {value})");
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_str(self, value: &str) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_str({self:#?}, {value})");
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_char(self, value: char) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_char({self:#?}, {value})");
        self.values.push(DbValue::from(value.to_string()));
        Ok(())
    }

    // Option types

    fn serialize_none(self) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_unit({self:#?})");
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        trace!("DbRowSerializer::serialize_some({self:#?}, ...)");
        value.serialize(self)
    }

    // Serializes an absent value (None)
    fn serialize_unit(self) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_unit({self:#?})");
        self.values.push(DbValue::Null);
        Ok(())
    }

    // More complex types:

    fn serialize_struct(
        self,
        _name: &str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        trace!("DbRowSerializer::serialize_struct({self:#?}, {_name}, {len})");
        if self.keys.len() > 0 {
            self.skip = len;
            // Start a new empty inner value which will be progressively filled in later:
            self.inner_value = json!({});
            Ok(self)
        } else {
            self.serialize_map(Some(len))
        }
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        trace!("DbRowSerializer::serialize_map({self:#?}, {_len:?})");
        Ok(self)
    }

    // A "unit" struct is another word for a struct that has no fields (I think "null" struct
    // would have been a better name).
    fn serialize_unit_struct(self, name: &str) -> Result<(), Self::Error> {
        trace!("DbRowSerializer::serialize_unit_struct({self:#?}, {name})");
        self.values.push(DbValue::from(name));
        Ok(())
    }

    fn serialize_unit_variant(
        self,
        _name: &str,
        _variant_index: u32,
        variant: &str,
    ) -> Result<(), Self::Error> {
        trace!(
            "DbRowSerializer::serialize_unit_variant(\
             {self:#?}, {_name}, {_variant_index}, {variant}\
             )"
        );
        // TODO: I don't like the extra double-quotes here,
        // but serde_json wants them for deserializing,
        // and I get a lifetime error when I try to add them
        // during the deserializing step.
        self.values.push(DbValue::from(format!(r#""{variant}""#)));
        Ok(())
    }

    // Unsupported types:

    fn serialize_bytes(self, _values: &[u8]) -> Result<(), Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing bytes is not supported".to_string(),
        ));
    }

    fn serialize_newtype_struct<T>(self, _name: &str, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "Serializing newtype struct is not supported".to_string(),
        ));
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &str,
        _variant_index: u32,
        _variant: &str,
        _value: &T,
    ) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "Serializing newtype variant is not supported".to_string(),
        ));
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing seq is not supported".to_string(),
        ));
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing tuple is not supported".to_string(),
        ));
    }

    fn serialize_tuple_struct(
        self,
        _name: &str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing tuple struct is not supported".to_string(),
        ));
    }

    fn serialize_tuple_variant(
        self,
        _name: &str,
        _variant_index: u32,
        _variant: &str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing tuple variant is not supported".to_string(),
        ));
    }

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
}

impl<'a> ser::SerializeStruct for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        trace!("SerializeStruct::serialize_field({self:#?}, {key}, ...)");
        if self.skip > 0 {
            self.skip -= 1;
            // self.keys.push(key.to_string());
            // self.skip_field(key)?;
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
        trace!("SerializeStruct::end({self:#?})");
        if self.inner_value != JsonValue::Null {
            let json_string = serde_json::to_string(&self.inner_value)
                .map_err(|err| DbError::SerdeError(err.to_string()))?;

            self.values.push(DbValue::Text(json_string));
            self.inner_value = JsonValue::Null;
        }
        Ok(())
    }
}

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

// Although a [DbRow] is essentially a wrapper around an IndexMap, when one is serialized,
// the serialization begins with our implementation of SerializeStruct and thus does not require
// us to explicitly implement actually working code for the implemetation of SerializeMap, because
// the map is serialized, instead, via the call to value.serialize() (see above).
impl<'a> ser::SerializeMap for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_key<T>(&mut self, _key: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeMap::serialize_key() is not supported for DbRowSerializer".to_string(),
        ));
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeMap::serialize_value() is not supported for DbRowSerializer".to_string(),
        ));
    }

    fn end(self) -> Result<(), DbError> {
        return Err(DbError::SerdeError(
            "SerializeMap::end() is not supported for DbRowSerializer".to_string(),
        ));
    }
}

impl<'a> ser::SerializeSeq for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeSeq::serialize_element() is not supported for DbRowSerializer".to_string(),
        ));
    }

    fn end(self) -> Result<(), Self::Error> {
        return Err(DbError::SerdeError(
            "SerializeSeq::end() is not supported for DbRowSerializer".to_string(),
        ));
    }
}

impl<'a> ser::SerializeTuple for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeTuple::serialize_element() is not supported for DbRowSerializer".to_string(),
        ));
    }

    fn end(self) -> Result<(), DbError> {
        return Err(DbError::SerdeError(
            "SerializeTuple::end() is not supported for DbRowSerializer".to_string(),
        ));
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeTupleStruct::serialize_field() is not supported for DbRowSerializer"
                .to_string(),
        ));
    }

    fn end(self) -> Result<(), DbError> {
        return Err(DbError::SerdeError(
            "SerializeTupleStruct::end() is not supported for DbRowSerializer".to_string(),
        ));
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut DbRowSerializer {
    // These need to match the `Ok` and `Error` types of DbRowSerializer:
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeTupleVariant::serialize_field() is not supported for DbRowSerializer"
                .to_string(),
        ));
    }

    fn end(self) -> Result<(), DbError> {
        return Err(DbError::SerdeError(
            "SerializeTupleVariant::end() is not supported for DbRowSerializer".to_string(),
        ));
    }
}

////////////////////////////////////////////////////////////////////////////////
// Deserialization implementations
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct DbRowDeserializer<'de> {
    /// The keys of the input [DbRow].
    keys: Vec<&'de str>,
    /// The values of the input [DbRow].
    values: Vec<&'de DbValue>,
}

impl<'de> DbRowDeserializer<'de> {
    pub(crate) fn from_db_row(input: &'de DbRow) -> Self {
        trace!("DbRowDeserializer::from_db_row({input:?})");
        DbRowDeserializer {
            keys: input.map.keys().map(|s| s.as_str()).collect::<Vec<_>>(),
            values: input.map.values().collect::<Vec<_>>(),
        }
    }

    fn pop_key(&mut self) -> Option<&'de str> {
        trace!("DbRowDeSerializer::pop_key({self:#?})");
        self.keys.pop()
    }

    fn pop_value(&mut self) -> Result<&'de DbValue, DbError> {
        trace!("DbRowDeSerializer::pop_value({self:#?})");
        self.values
            .pop()
            .ok_or(DbError::SerdeError("No more values to pop".to_string()))
    }

    fn last_value(&self) -> Result<&DbValue, DbError> {
        trace!("DbRowDeSerializer::last_value({self:#?})");
        self.values
            .last()
            .ok_or(DbError::SerdeError("No more values".to_string()))
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
        trace!("DbRowDeSerializer::deserialize_bool({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_i8({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_i16({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_i32({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_i64({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_u8({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_u16({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_u32({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_u64({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_f32({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_f64({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_string({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_char({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_str({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_any({self:#?}, ...)");
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
            },
        }
    }

    // Options:

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        trace!("DbRowDeSerializer::deserialize_option({self:#?}, ...)");
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
        trace!("DbRowDeSerializer::deserialize_unit({self:#?}, ...)");
        let value = self.pop_value()?;
        if *value != DbValue::Null {
            return Err(DbError::SerdeError("Expected NULL".to_string()));
        }
        visitor.visit_unit()
    }

    // More complex types:

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        trace!("DbRowDeSerializer::deserialize_struct({self:#?}, ...)");
        if self.keys == fields {
            self.deserialize_map(visitor)
        } else {
            let value = self.pop_value()?;
            match value.as_str() {
                Some(value) => serde_json::Deserializer::from_str(value)
                    .deserialize_struct(name, fields, visitor)
                    .map_err(|err| DbError::SerdeError(err.to_string())),
                None => Err(DbError::SerdeError(format!("Not a string: {value:?}"))),
            }
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        trace!("DbRowDeSerializer::deserialize_map({self:#?}, ...)");
        visitor.visit_map(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        trace!("DbRowDeSerializer::deserialize_identifier({self:#?}, ...)");
        let key = self
            .pop_key()
            .ok_or(DbError::SerdeError("No more keys".to_string()))?;
        visitor.visit_borrowed_str(key)
    }

    // A "unit" struct is another word for a struct that has no fields (I think "null" struct
    // would have been a better name).
    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        trace!("DbRowDeSerializer::deserialize_unit_struct({self:#?}, ...)");
        self.pop_value()?;
        visitor.visit_unit()
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
        trace!("DbRowDeSerializer::deserialize_enum({self:#?}, ...)");
        let value = self.pop_value()?;
        match value.as_str() {
            Some(value) => serde_json::Deserializer::from_str(&value)
                .deserialize_enum(name, variants, visitor)
                .map_err(|err| DbError::SerdeError(err.to_string())),
            None => Err(DbError::SerdeError(format!("Not a string: {value:?}"))),
        }
    }

    // Unsupported types:

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

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'newtype_struct' is not supported".to_string(),
        ));
    }

    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'seq' is not supported".to_string(),
        ));
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'tuple' is not supported".to_string(),
        ));
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'tuple_struct' is not supported".to_string(),
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
        trace!("DbRowDeSerializer::next_key_seed({self:#?}, ...)");
        if self.keys.len() == 0 {
            return Ok(None);
        }
        seed.deserialize(&mut *self).map(Some)
    }

    fn next_value_seed<S>(&mut self, seed: S) -> Result<S::Value, Self::Error>
    where
        S: de::DeserializeSeed<'de>,
    {
        trace!("DbRowDeSerializer::next_value_seed({self:#?}, ...)");
        seed.deserialize(&mut *self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db_row, db_value::DbValue};
    use rust_decimal::{Decimal, dec};
    use serde::Deserialize;
    use tracing_test::traced_test;

    #[test]
    #[traced_test]
    fn test_serde_struct() {
        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct UnitStruct;

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        enum TrivialEnum {
            UnitStruct,
        }

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct TrivialStruct {
            foo: String,
        }

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct NestedStruct {
            bar: TrivialStruct,
        }

        // Serializing and deserializing an arbitrary struct to a DbRow:
        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct TestStruct {
            boolean: bool,
            boolean_opt_none: Option<bool>,
            boolean_opt_some: Option<bool>,
            tinyint: i8,
            tinyint_opt_none: Option<i8>,
            tinyint_opt_some: Option<i8>,
            tiny_unsigned: u8,
            tiny_unsigned_opt_none: Option<u8>,
            tiny_unsigned_opt_some: Option<u8>,
            smallint: i16,
            smallint_opt_none: Option<i16>,
            smallint_opt_some: Option<i16>,
            small_unsigned: u16,
            small_unsigned_opt_none: Option<u16>,
            small_unsigned_opt_some: Option<u16>,
            mediumint: i32,
            mediumint_opt_none: Option<i32>,
            mediumint_opt_some: Option<i32>,
            medium_unsigned: u32,
            medium_unsigned_opt_none: Option<u32>,
            medium_unsigned_opt_some: Option<u32>,
            bigint: i64,
            bigint_opt_none: Option<i64>,
            bigint_opt_some: Option<i64>,
            big_unsigned: u64,
            big_unsigned_opt_none: Option<u64>,
            big_unsigned_opt_some: Option<u64>,
            smallfloat: f32,
            smallfloat_opt_none: Option<f32>,
            smallfloat_opt_some: Option<f32>,
            bigfloat: f64,
            bigfloat_opt_none: Option<f64>,
            bigfloat_opt_some: Option<f64>,
            text: String,
            text_opt_none: Option<String>,
            text_opt_some: Option<String>,
            // TODO: Decimals are only sort-of supported for now, i.e., rust's serializer
            // serializes them to text (see also below). This is not ideal but at least it's
            // consistent.
            biggerfloat: Decimal,
            biggerfloat_opt_none: Option<Decimal>,
            biggerfloat_opt_some: Option<Decimal>,
            // Nested types:
            unit_struct: UnitStruct,
            unit_struct_opt_none: Option<UnitStruct>,
            unit_struct_opt_some: Option<UnitStruct>,
            enum_field: TrivialEnum,
            enum_field_opt_none: Option<TrivialEnum>,
            enum_field_opt_some: Option<TrivialEnum>,
            struct_field: TrivialStruct,
            struct_field_opt_none: Option<TrivialStruct>,
            struct_field_opt_some: Option<TrivialStruct>,
            nested_struct: NestedStruct,
            nested_struct_opt_none: Option<NestedStruct>,
            nested_struct_opt_some: Option<NestedStruct>,
        }

        let expected_struct = TestStruct {
            boolean: true,
            boolean_opt_none: None,
            boolean_opt_some: Some(true),

            tinyint: 1,
            tinyint_opt_none: None,
            tinyint_opt_some: Some(1),

            tiny_unsigned: 1,
            tiny_unsigned_opt_none: None,
            tiny_unsigned_opt_some: Some(1),

            smallint: 1,
            smallint_opt_none: None,
            smallint_opt_some: Some(1),

            small_unsigned: 1,
            small_unsigned_opt_none: None,
            small_unsigned_opt_some: Some(1),

            mediumint: 1,
            mediumint_opt_none: None,
            mediumint_opt_some: Some(1),

            medium_unsigned: 1,
            medium_unsigned_opt_none: None,
            medium_unsigned_opt_some: Some(1),

            bigint: 1,
            bigint_opt_none: None,
            bigint_opt_some: Some(1),

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

            // Nested types:
            unit_struct: UnitStruct,
            unit_struct_opt_none: None,
            unit_struct_opt_some: Some(UnitStruct),

            enum_field: TrivialEnum::UnitStruct,
            enum_field_opt_none: None,
            enum_field_opt_some: Some(TrivialEnum::UnitStruct),

            struct_field: TrivialStruct {
                foo: String::from("bar"),
            },
            struct_field_opt_none: None,
            struct_field_opt_some: Some(TrivialStruct {
                foo: String::from("bar"),
            }),

            nested_struct: NestedStruct {
                bar: TrivialStruct {
                    foo: String::from("bar"),
                },
            },
            nested_struct_opt_none: None,
            nested_struct_opt_some: Some(NestedStruct {
                bar: TrivialStruct {
                    foo: String::from("bar"),
                },
            }),
        };

        let expected_db_row = db_row! {
            "boolean" => true,
            "boolean_opt_none" => DbValue::Null,
            "boolean_opt_some" => true,
            "tinyint" => 1_i16,
            "tinyint_opt_none" => DbValue::Null,
            "tinyint_opt_some" => 1_i16,
            "tiny_unsigned" => 1_i16,
            "tiny_unsigned_opt_none" => DbValue::Null,
            "tiny_unsigned_opt_some" => 1_i16,
            "smallint" => 1_i16,
            "smallint_opt_none" => DbValue::Null,
            "smallint_opt_some" => 1_i16,
            "small_unsigned" => 1_i32,
            "small_unsigned_opt_none" => DbValue::Null,
            "small_unsigned_opt_some" => 1_i32,
            "mediumint" => 1_i32,
            "mediumint_opt_none" => DbValue::Null,
            "mediumint_opt_some" => 1_i32,
            "medium_unsigned" => 1_u64,
            "medium_unsigned_opt_none" => DbValue::Null,
            "medium_unsigned_opt_some" => 1_u64,
            "bigint" => 1_i64,
            "bigint_opt_none" => DbValue::Null,
            "bigint_opt_some" => 1_i64,
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
            // Nested types:
            "unit_struct" => "UnitStruct",
            "unit_struct_opt_none" => DbValue::Null,
            "unit_struct_opt_some" => "UnitStruct",
            "enum_field" => "\"UnitStruct\"",
            "enum_field_opt_none" => DbValue::Null,
            "enum_field_opt_some" => "\"UnitStruct\"",
            "struct_field" => "{\"foo\":\"bar\"}",
            "struct_field_opt_none" => DbValue::Null,
            "struct_field_opt_some" => "{\"foo\":\"bar\"}",
            "nested_struct" => "{\"bar\":{\"foo\":\"bar\"}}",
            "nested_struct_opt_none" => DbValue::Null,
            "nested_struct_opt_some" => "{\"bar\":{\"foo\":\"bar\"}}",
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
