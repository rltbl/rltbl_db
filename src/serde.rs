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
use serde_json::json;

/// Convert the given supported struct to a [DbRow]. For this to be successful, the struct
/// must be a "normal struct" of the form:
///
/// ```ignore
/// struct NormalStruct {
///   field1: type1, // or Option<type1>
///   field2: type2, // or Option<type2>
///   ...
/// }
/// ```
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
    let mut serializer = DbRowSerializer::new();
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
/// ```ignore
/// struct NormalStruct {
///   field1: type1, // or Option<type1>
///   field2: type2, // or Option<type2>
///   ...
/// }
/// ```
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
        Err(DbError::SerdeError(format!(
            "Deserialization error: Leftover keys: {:?} and/or values: {:?}",
            deserializer.keys, deserializer.values
        )))
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

#[derive(Debug, Default, Clone)]
struct NestedType {
    _name: String,
    len: usize,
}

#[derive(Debug, Default)]
struct DbRowSerializer {
    /// The keys of the output [DbRow].
    keys: Vec<String>,
    /// The values of the output [DbRow].
    values: Vec<DbValue>,

    /// TODO: Add docstrings for all of these.
    nesting_type: String,

    nested_types: Vec<NestedType>,

    nested_keys: Vec<String>,
    nested_values: Vec<JsonValue>,
}

impl DbRowSerializer {
    fn new() -> Self {
        DbRowSerializer::default()
    }

    fn push_nested_row(&mut self) -> Result<(), DbError> {
        assert_eq!(self.nested_keys.len(), self.nested_values.len());
        let mut nested_row = JsonRow::new();
        for (i, key) in self.nested_keys.iter().enumerate() {
            nested_row.insert(key.to_string(), self.nested_values[i].clone());
        }
        self.values
            .push(DbValue::Text(json!(nested_row).to_string()));
        self.nested_types.pop();
        self.nested_keys.clear();
        self.nested_values.clear();
        Ok(())
    }

    fn push_to_nested_values(&mut self, value: JsonValue) -> Result<(), DbError> {
        self.nested_values.push(value);
        let num_types = self.nested_types.len();
        if let Some(ref mut nested) = self.nested_types.get_mut(num_types - 1) {
            nested.len -= 1;
        }
        if let Some(ntype) = &self.nested_types.last() {
            if ntype.len == 0 {
                self.push_nested_row()?;
            }
        }
        Ok(())
    }
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

    // Primitive types

    fn serialize_bool(self, value: bool) -> Result<(), Self::Error> {
        // TODO: These printlns are useful but they should be replaced with tracing statements.
        //println!("In serialize_bool with SELF: {self:#?}");
        self.values.push(DbValue::from(value));
        // println!("SELF IS NOW: {self:#?}");
        Ok(())
    }

    fn serialize_i8(self, value: i8) -> Result<(), Self::Error> {
        //println!("In serialize_i8 with SELF: {self:#?}");
        self.values.push(DbValue::from(value));
        // println!("SELF IS NOW: {self:#?}");
        Ok(())
    }

    fn serialize_i16(self, value: i16) -> Result<(), Self::Error> {
        //println!("In serialize_i16 with SELF: {self:#?}");

        if self
            .nested_types
            .last()
            .clone()
            .and_then(|ntype| Some(ntype.len))
            .unwrap_or(0)
            == 0
        {
            self.values.push(DbValue::from(value));
        } else {
            self.push_to_nested_values(json!(value))?;
        }

        // println!("SELF IS NOW: {self:#?}");
        Ok(())
    }

    fn serialize_i32(self, value: i32) -> Result<(), Self::Error> {
        //println!("In serialize_i32 with SELF: {self:#?}");
        self.values.push(DbValue::from(value));
        // println!("SELF IS NOW {self:#?}");
        Ok(())
    }

    fn serialize_i64(self, value: i64) -> Result<(), Self::Error> {
        //println!("In serialize_i64 with SELF: {self:#?}");
        self.values.push(DbValue::from(value));
        // println!("SELF IS NOW {self:#?}");
        Ok(())
    }

    fn serialize_u8(self, value: u8) -> Result<(), Self::Error> {
        //println!("In serialize_u8 with SELF: {self:#?}");
        self.values.push(DbValue::from(value));
        // println!("SELF IS NOW {self:#?}");
        Ok(())
    }

    fn serialize_u16(self, value: u16) -> Result<(), Self::Error> {
        //println!("In serialize_u16 with SELF: {self:#?}");
        self.values.push(DbValue::from(value));
        // println!("SELF IS NOW {self:#?}");
        Ok(())
    }

    fn serialize_u32(self, value: u32) -> Result<(), Self::Error> {
        //println!("In serialize_u32 with SELF: {self:#?}");
        self.values.push(DbValue::from(value));
        // println!("SELF IS NOW {self:#?}");
        Ok(())
    }

    fn serialize_u64(self, value: u64) -> Result<(), Self::Error> {
        //println!("In serialize_u64 with SELF: {self:#?}");
        self.values.push(DbValue::from(value));
        // println!("SELF IS NOW {self:#?}");
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> Result<(), Self::Error> {
        //println!("In serialize_f32 with SELF: {self:#?}");
        self.values.push(DbValue::from(value));
        // println!("SELF IS NOW: {self:#?}");
        Ok(())
    }

    fn serialize_f64(self, value: f64) -> Result<(), Self::Error> {
        //println!("In serialize_f64 with SELF: {self:#?}");
        self.values.push(DbValue::from(value));
        // println!("SELF IS NOW {self:#?}");
        Ok(())
    }

    fn serialize_str(self, value: &str) -> Result<(), Self::Error> {
        //println!("In serialize_str with SELF: {self:#?}");
        self.values.push(DbValue::from(value));
        // println!("SELF IS NOW {self:#?}");
        Ok(())
    }

    fn serialize_char(self, value: char) -> Result<(), Self::Error> {
        //println!("In serialize_value with SELF: {self:#?}");
        self.values.push(DbValue::from(value.to_string()));
        // println!("SELF IS NOW {self:#?}");
        Ok(())
    }

    // Options

    fn serialize_none(self) -> Result<(), Self::Error> {
        //println!("In serialize_none with SELF: {self:#?}");
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        //println!("In serialize_some with SELF: {self:#?}");
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<(), Self::Error> {
        //println!("In serialize_unit with SELF: {self:#?}");
        self.values.push(DbValue::Null);
        // println!("SELF IS NOW {self:#?}");
        Ok(())
    }

    // More complex types:

    fn serialize_struct(
        self,
        name: &str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        //println!("In serialize_struct with NAME: {name}, LEN: {len}, and SELF: {self:#?}");
        if self.nesting_type == "" {
            self.nesting_type = name.to_string();
        }

        if name != self.nesting_type {
            if let None = self.nested_types.last() {
                self.nested_types.push(NestedType {
                    _name: name.to_string(),
                    len,
                });
            } else {
                // TODO: We should be able to support this:
                panic!("Can't nest another type");
            }
        }
        self.serialize_map(Some(len))
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        //println!("In serialize_map with LEN: {len:?}");
        Ok(self)
    }

    // Unsupported types:

    fn serialize_bytes(self, _values: &[u8]) -> Result<(), Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing bytes is not supported".to_string(),
        ));
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
        //println!("In SerializeStruct::serialize_field: {self:#?}");
        if self
            .nested_types
            .last()
            .clone()
            .and_then(|ntype| Some(ntype.len))
            .unwrap_or(0)
            == 0
        {
            self.keys.push(key.to_string());
        } else {
            self.nested_keys.push(key.to_string());
        }
        value.serialize(&mut **self)?;
        // println!("SELF IS NOW: {self:#?}");
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
struct DbRowDeserializer<'de> {
    /// The keys of the input [DbRow].
    keys: Vec<&'de str>,
    /// The values of the input [DbRow].
    values: Vec<&'de DbValue>,
}

impl<'de> DbRowDeserializer<'de> {
    fn from_db_row(input: &'de DbRow) -> Self {
        DbRowDeserializer {
            keys: input.map.keys().map(|s| s.as_str()).collect::<Vec<_>>(),
            values: input.map.values().collect::<Vec<_>>(),
        }
    }

    fn pop_key(&mut self) -> Option<&'de str> {
        self.keys.pop()
    }

    fn pop_value(&mut self) -> Option<&'de DbValue> {
        self.values.pop()
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut DbRowDeserializer<'de> {
    type Error = DbError;

    // Primitives:

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

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value().unwrap();
                let value = value.as_i8().unwrap();
                visitor.visit_i8(value)
            }
        }
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

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value().unwrap();
                let value = value.as_u8().unwrap();
                visitor.visit_u8(value)
            }
        }
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value().unwrap();
                let value = value.as_u16().unwrap();
                visitor.visit_u16(value)
            }
        }
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value().unwrap();
                let value = value.as_u32().unwrap();
                visitor.visit_u32(value)
            }
        }
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => {
                let value = self.pop_value().unwrap();
                let value = value.as_u64().unwrap();
                visitor.visit_u64(value)
            }
        }
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

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, DbError>
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

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, DbError>
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

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.values.last().unwrap() {
            DbValue::Null => self.deserialize_unit(visitor),
            _ => match self.pop_value().unwrap() {
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
        //println!("deserialize_struct {name}");
        if self.keys == fields {
            self.deserialize_map(visitor)
        } else {
            let value = self.pop_value().unwrap().as_str().unwrap();
            serde_json::Deserializer::from_str(value)
                .deserialize_struct(name, fields, visitor)
                .map_err(|err| DbError::SerdeError(err.to_string()))
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
        // TODO: Remove unwraps here and elsewhere.
        let key = self.pop_key().unwrap();
        visitor.visit_borrowed_str(key)
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
    use rust_decimal::{Decimal, dec};
    use serde::Deserialize;

    #[test]
    fn test_serde_normal_struct() {
        // Serializing and deserializing an arbitrary struct to a DbRow:
        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct NormalStruct {
            // TODO: For completeness add all of the possible field types here:
            boolean: bool,
            boolean_opt: Option<bool>,
            tinyint: i8,
            tinyint_opt: Option<i8>,
            tiny_unsigned: u8,
            tiny_unsigned_opt: Option<u8>,
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
            text: String,
            text_opt: Option<String>,
            // TODO: Decimals are only sort-of supported for now, i.e., rust's serializer
            // serializes them to text (see also below). This is not ideal but at least it's
            // consistent.
            biggerfloat: Decimal,
        }

        let expected_struct = NormalStruct {
            boolean: true,
            boolean_opt: Some(true),
            tinyint: 1,
            tinyint_opt: Some(1),
            tiny_unsigned: 1,
            tiny_unsigned_opt: None,
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
            text: 1.to_string(),
            text_opt: Some(1.to_string()),
            biggerfloat: dec!(1),
        };

        let expected_db_row = db_row! {
            "boolean" => true,
            "boolean_opt" => true,
            "tinyint" => 1_i16,
            "tinyint_opt" => 1_i16,
            "tiny_unsigned" => 1_i16,
            "tiny_unsigned_opt" => DbValue::Null,
            "smallint" => 1_i16,
            "smallint_opt" => DbValue::Null,
            "mediumint" => 1_i32,
            "mediumint_opt" => 1_i32,
            "bigint" => 1_i64,
            "bigint_opt" => DbValue::Null,
            "smallfloat" => 1_f32,
            "smallfloat_opt" => 1_f32,
            "bigfloat" => 1_f64,
            "bigfloat_opt" => DbValue::Null,
            "text" => "1",
            "text_opt" => "1",
            // Serde interprets Decimals as text:
            "biggerfloat" => "1",
        };
        assert_eq!(expected_db_row, to_db_row(&expected_struct).unwrap());
        assert_eq!(expected_struct, from_db_row(&expected_db_row).unwrap());
        assert_eq!(
            expected_struct,
            from_db_row_indirect(&expected_db_row).unwrap()
        );
    }

    #[test]
    fn test_serde_nested_struct() {
        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct NestedStruct {
            foo: i16,
            bar: i16,
        }

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct NestingStruct {
            alpha: i16,
            beta: i16,
            gamma: NestedStruct,
            delta: i16,
            epsilon: NestedStruct,
        }

        let expected_struct = NestingStruct {
            alpha: 1_i16,
            beta: 1_i16,
            gamma: NestedStruct { foo: 12, bar: 13 },
            delta: 1_i16,
            epsilon: NestedStruct { foo: 14, bar: 15 },
        };

        let expected_db_row = db_row! {
            "alpha" => 1_i16,
            "beta" => 1_i16,
            "gamma" => r#"{"foo":12,"bar":13}"#,
            "delta" => 1_i16,
            "epsilon" => r#"{"foo":14,"bar":15}"#,
        };

        assert_eq!(expected_db_row, to_db_row(&expected_struct).unwrap());
        assert_eq!(expected_struct, from_db_row(&expected_db_row).unwrap());
    }
}
