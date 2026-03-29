//! Serialization / deserialization implementations for rltbl_db.

use crate::{
    core::DbError,
    db_value::{DbRow, DbValue, JsonRow, JsonValue},
};
use rust_decimal::prelude::ToPrimitive;
use serde::{
    Deserialize, Serialize,
    de::{self, Visitor},
    ser,
};

/// Convert the given struct to a [DbRow]. For this to be successful, the struct
/// must be a "normal struct" of the form:
///
/// struct NormalStruct {
///   field1: type1,
///   field2: type2,
///   ...
/// }
///
/// where type1, type2, ... are among the primitive types associated with
/// the different kinds of [DbValue]. Other field types, and other types of
/// structs (e.g., tuple structs, unit structs, see
/// <https://doc.rust-lang.org/book/ch05-01-defining-structs.html>) are not
/// supported and attempts to serialize them will result in an error, as will
/// attempts to serialize anything other than a struct (e.g., an enum).
pub fn to_db_row<T>(value: &T) -> Result<DbRow, DbError>
where
    T: Serialize,
{
    let mut serializer = DbRowSerializer {
        keys: vec![],
        values: vec![],
    };
    value.serialize(&mut serializer)?;
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
/// struct NormalStruct {
///   field1: type1,
///   field2: type2,
///   ...
/// }
///
/// where type1, type2, ... are among the primitive types associated with
/// the different kinds of [DbValue]. Other field types, and other types of
/// structs (e.g., tuple structs, unit structs, see
/// <https://doc.rust-lang.org/book/ch05-01-defining-structs.html>) are not
/// supported and attempts to deserialize a [DbRow] to them will result in an
/// error, as will attempts to deserialize a [DbRow] to anything other than a
/// struct (e.g., an enum).
pub fn from_db_row<T>(db_row: &DbRow) -> Result<T, DbError>
where
    T: for<'de> Deserialize<'de>,
{
    let mut deserializer = DbRowDeserializer::from_db_row(db_row);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.keys.is_empty() && deserializer.values.is_empty() {
        Ok(t)
    } else {
        Err(DbError::SerdeError(
            "Deserialization error: Leftover input".to_string(),
        ))
    }
}

// TODO: Remove this alternative implementation of from_db_row() before merging this branch.
// Keeping it around for now for comparison with the preferred implementaion.
pub fn from_db_row_indirect<T>(db_row: &DbRow) -> Result<T, DbError>
where
    T: for<'a> Deserialize<'a>,
{
    // The method below *works* and, unlike the case of serializing a struct to a DbRow, where
    // converting to JSON as an intermediate step necessarily throws away the type information
    // that we need for a successful serialization to DbRow, in the case of deserialization,
    // converting to JSON as an intermediate step is not lossy in that sense. So we do not
    // *need* to find an alternative method. However converting to JSON first seems
    // inefficient and it is probably be better to deserialize directly from a DbRow
    // to a T struct, as is done in from_db_row() above.
    let mut flat_row = JsonRow::new();
    for (column, value) in db_row.iter() {
        match value {
            DbValue::Null => flat_row.insert(column.to_string(), JsonValue::Null),
            DbValue::Boolean(num) => flat_row.insert(column.to_string(), JsonValue::from(*num)),
            DbValue::SmallInteger(num) => {
                flat_row.insert(column.to_string(), JsonValue::from(*num))
            }
            DbValue::Integer(num) => flat_row.insert(column.to_string(), JsonValue::from(*num)),
            DbValue::BigInteger(num) => flat_row.insert(column.to_string(), JsonValue::from(*num)),
            DbValue::Real(num) => flat_row.insert(column.to_string(), JsonValue::from(*num)),
            DbValue::BigReal(num) => flat_row.insert(column.to_string(), JsonValue::from(*num)),
            DbValue::Numeric(num) => {
                flat_row.insert(column.to_string(), JsonValue::from(num.to_f64()))
            }
            DbValue::Text(txt) => {
                flat_row.insert(column.to_string(), JsonValue::from(txt.to_string()))
            }
        };
    }
    let t_struct: T = serde_json::from_str(&serde_json::json!(flat_row).to_string())
        .map_err(|err| DbError::SerdeError(err.to_string()))?;
    Ok(t_struct)
}

////////////////////////////////////////////////////////////////////////////////
// Serialization implementations
////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
struct DbRowSerializer {
    /// The keys of the output [DbRow].
    keys: Vec<String>,
    /// The values of the output [DbRow].
    values: Vec<DbValue>,
}

impl<'a> ser::Serializer for &'a mut DbRowSerializer {
    // The output type produced by this `DbRowSerializer` during successful
    // serialization.
    type Ok = ();

    // The error type when some error occurs during serialization.
    type Error = DbError;

    // Associated types for keeping track of additional state while serializing
    // compound data structures like sequences and maps. In this case no
    // additional state is required beyond what is already stored in the
    // DbRowSerializer struct.
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, value: bool) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_i8(self, _value: i8) -> Result<(), Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing i8 is not supported".to_string(),
        ));
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

    fn serialize_u8(self, _value: u8) -> Result<(), Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing u8 is not supported".to_string(),
        ));
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

    fn serialize_char(self, _value: char) -> Result<(), Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing char is not supported".to_string(),
        ));
    }

    fn serialize_str(self, value: &str) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_bytes(self, _values: &[u8]) -> Result<(), Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing bytes is not supported".to_string(),
        ));
    }

    fn serialize_none(self) -> Result<(), Self::Error> {
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), Self::Error> {
        self.values.push(DbValue::Null);
        Ok(())
    }

    fn serialize_unit_struct(self, _name: &str) -> Result<(), Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing unit struct is not supported".to_string(),
        ));
    }

    fn serialize_unit_variant(
        self,
        _name: &str,
        _variant_index: u32,
        _variant: &str,
    ) -> Result<(), Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing unit variant is not supported".to_string(),
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

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(self)
    }

    fn serialize_struct(
        self,
        _name: &str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.serialize_map(Some(len))
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
        self.keys.push(key.to_string());
        value.serialize(&mut **self)?;
        Ok(())
    }

    fn end(self) -> Result<(), DbError> {
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
pub struct DbRowDeserializer<'de> {
    /// The keys of the input [DbRow].
    keys: Vec<&'de str>,
    /// The values of the input [DbRow].
    values: Vec<&'de DbValue>,
}

impl<'de> DbRowDeserializer<'de> {
    pub fn from_db_row(input: &'de DbRow) -> Self {
        DbRowDeserializer {
            keys: input.map.keys().map(|s| s.as_str()).collect::<Vec<_>>(),
            values: input.map.values().collect::<Vec<_>>(),
        }
    }

    pub fn pop_key(&mut self) -> Option<&'de str> {
        self.keys.pop()
    }

    pub fn pop_value(&mut self) -> Option<&'de DbValue> {
        self.values.pop()
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut DbRowDeserializer<'de> {
    type Error = DbError;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'any' is not supported".to_string(),
        ));
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value().unwrap();
                let value = value.as_bool().unwrap();
                visitor.visit_bool(value)
            }
        }
    }

    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'i8' is not supported".to_string(),
        ));
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value().unwrap();
                let value = value.as_i16().unwrap();
                visitor.visit_i16(value)
            }
        }
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value().unwrap();
                let value = value.as_i32().unwrap();
                visitor.visit_i32(value)
            }
        }
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value().unwrap();
                let value = value.as_i64().unwrap();
                visitor.visit_i64(value)
            }
        }
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'u8' is not supported".to_string(),
        ));
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'u16' is not supported".to_string(),
        ));
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'u32' is not supported".to_string(),
        ));
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'u64' is not supported".to_string(),
        ));
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value().unwrap();
                let value = value.as_f32().unwrap();
                visitor.visit_f32(value)
            }
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value().unwrap();
                let value = value.as_f64().unwrap();
                visitor.visit_f64(value)
            }
        }
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'char' is not supported".to_string(),
        ));
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'str' is not supported".to_string(),
        ));
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value().unwrap();
                let value = value.as_str().unwrap();
                visitor.visit_borrowed_str(value)
            }
        }
    }

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

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        let value = self.values.last().unwrap();
        match value {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        let value = self.pop_value().unwrap();
        // TODO: Remove the assert
        assert_eq!(*value, DbValue::Null);
        visitor.visit_unit()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'unit_struct' is not supported".to_string(),
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

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        return Err(DbError::SerdeError(
            "Deserializing 'enum' is not supported".to_string(),
        ));
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // TODO: Remove unwraps here and elsewhere.
        let key = self.pop_key().unwrap();
        visitor.visit_borrowed_str(key)
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
    use crate::{db_row, db_value::DbValue};
    use serde::Deserialize;
    // use rust_decimal::{Decimal, dec};

    #[test]
    fn test_serde_struct() {
        // Serializing and deserializing an arbitrary struct to a DbRow:
        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct TestStruct {
            boolean: bool,
            boolean_opt: Option<bool>,
            smallint: i16,
            smallint_opt: Option<i16>,
            mediumint: i32,
            mediumint_opt: Option<i32>,
            bigint: i64,
            bigint_opt: Option<i64>,
            smallfloat: f32,
            smallfloat_opt: Option<f32>,
            bigfloat: f64,
            bigfloat_opt: Option<f64>,
            // biggerfloat: Decimal,
            text: String,
            text_opt: Option<String>,
        }

        let expected_struct = TestStruct {
            boolean: true,
            boolean_opt: Some(true),
            smallint: 1,
            smallint_opt: None,
            mediumint: 1,
            mediumint_opt: Some(1),
            bigint: 1,
            bigint_opt: None,
            smallfloat: 1_f32,
            smallfloat_opt: Some(1_f32),
            bigfloat: 1_f64,
            bigfloat_opt: None,
            // biggerfloat: dec!(1),
            text: 1.to_string(),
            text_opt: Some(1.to_string()),
        };

        let expected_db_row = db_row! {
            "boolean".into() => DbValue::from(true),
            "boolean_opt".into() => DbValue::from(true),
            "smallint".into() => DbValue::from(1_i16),
            "smallint_opt".into() => DbValue::Null,
            "mediumint".into() => DbValue::from(1_i32),
            "mediumint_opt".into() => DbValue::from(1_i32),
            "bigint".into() => DbValue::from(1_i64),
            "bigint_opt".into() => DbValue::Null,
            "smallfloat".into() => DbValue::from(1_f32),
            "smallfloat_opt".into() => DbValue::from(1_f32),
            "bigfloat".into() => DbValue::from(1_f64),
            "bigfloat_opt".into() => DbValue::Null,
            // "biggerfloat".into() => DbValue::from(1_i64),
            "text".into() => DbValue::from("1"),
            "text_opt".into() => DbValue::from("1"),
        };
        assert_eq!(expected_db_row, to_db_row(&expected_struct).unwrap());
        assert_eq!(expected_struct, from_db_row(&expected_db_row).unwrap());
        //assert_eq!(
        //    expected_struct,
        //    from_db_row_indirect(&expected_db_row).unwrap()
        //);
    }
}
