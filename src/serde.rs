//! Implement `serde` for values and rows.

use crate::{Error, JsonValue, Value};
use serde::{
    Deserialize, Serialize,
    de::{self, Visitor},
    ser,
};
use serde_json::{Map as JsonMap, json, value::Serializer as JsonValueSerializer};

////////////////////////////////////////////////////////////////////////////////
// Serialization implementations
////////////////////////////////////////////////////////////////////////////////

struct ValueSerializer;

/// TODO: Add docstring.
pub fn to_value<T>(value: &T) -> Result<Value, Error>
where
    T: Serialize,
{
    // Serialize the given value.
    let serializer = ValueSerializer;
    value.serialize(&serializer)
}

// TODO: We probably don't need the lifetime.
impl<'a> ser::Serializer for &'a ValueSerializer {
    // The output type produced by this serializer during successful serialization.
    type Ok = Value;

    // The error type when some error occurs during serialization.
    type Error = Error;

    // Associated types for keeping track of additional state while serializing compound data
    // structures like sequences and maps. In this case no additional state is required beyond
    // what is already stored in the Value struct.
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    // Primitive types
    fn serialize_bool(self, value: bool) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }

    fn serialize_i8(self, value: i8) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }

    fn serialize_i16(self, value: i16) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }

    fn serialize_i32(self, value: i32) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }

    fn serialize_i64(self, value: i64) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }

    fn serialize_u8(self, value: u8) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }

    fn serialize_u16(self, value: u16) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }

    fn serialize_u32(self, value: u32) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }

    fn serialize_u64(self, value: u64) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }

    fn serialize_f32(self, value: f32) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }

    fn serialize_f64(self, value: f64) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }

    fn serialize_str(self, value: &str) -> Result<Value, Self::Error> {
        Ok(Value::from(value))
    }

    fn serialize_char(self, value: char) -> Result<Value, Self::Error> {
        Ok(Value::from(value.to_string()))
    }

    // Option types

    fn serialize_none(self) -> Result<Value, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<Value, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    // Serializes an absent _value (None)
    fn serialize_unit(self) -> Result<Value, Self::Error> {
        Ok(Value::Null)
    }

    // Compound types:

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        todo!()
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        todo!()
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        todo!()
    }

    fn serialize_struct(
        self,
        _name: &str,
        _len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        todo!()
    }

    // A "unit" struct has no fields.
    fn serialize_unit_struct(self, _name: &str) -> Result<Value, Self::Error> {
        todo!()
    }

    fn serialize_newtype_struct<T>(self, _name: &str, _value: &T) -> Result<Value, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_tuple_struct(
        self,
        _name: &str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        todo!()
    }

    fn serialize_unit_variant(
        self,
        _name: &str,
        _variant_index: u32,
        _variant: &str,
    ) -> Result<Value, Self::Error> {
        todo!()
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &str,
        _variant_index: u32,
        _variant: &str,
        _value: &T,
    ) -> Result<Value, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_tuple_variant(
        self,
        _name: &str,
        _variant_index: u32,
        _variant: &str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        todo!()
    }

    // Unsupported compound types

    fn serialize_struct_variant(
        self,
        _name: &str,
        _variant_index: u32,
        _variant: &str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        todo!()
    }

    // Although we don't support fields with type &[u8], bytes fields are supported as long
    // as they are owned, i.e., Vec<u8>, throush serialize_seq().
    fn serialize_bytes(self, _values: &[u8]) -> Result<Value, Self::Error> {
        todo!()
    }
}

impl<'a> ser::SerializeStruct for &'a ValueSerializer {
    // These need to match the `Ok` and `Error` types of Value:
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Value, Error> {
        todo!()
    }
}

impl<'a> ser::SerializeTupleStruct for &'a ValueSerializer {
    // These need to match the `Ok` and `Error` types of Value:
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Value, Error> {
        todo!()
    }
}

impl<'a> ser::SerializeTuple for &'a ValueSerializer {
    // These need to match the `Ok` and `Error` types of Value:
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Value, Error> {
        todo!()
    }
}

impl<'a> ser::SerializeTupleVariant for &'a ValueSerializer {
    // These need to match the `Ok` and `Error` types of Value:
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Value, Error> {
        todo!()
    }
}

impl<'a> ser::SerializeSeq for &'a ValueSerializer {
    // These need to match the `Ok` and `Error` types of Value:
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Value, Self::Error> {
        todo!()
    }
}

// This might be called when serializing a JSON value that happens to be a map.
impl<'a> ser::SerializeMap for &'a ValueSerializer {
    // These need to match the `Ok` and `Error` types of Value:
    type Ok = Value;
    type Error = Error;

    fn serialize_key<T>(&mut self, _key: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Value, Error> {
        todo!()
    }
}

// Unsupported implementations

impl<'a> ser::SerializeStructVariant for &'a ValueSerializer {
    // These need to match the `Ok` and `Error` types of Value:
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Value, Error> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////
// Deserialization implementations
////////////////////////////////////////////////////////////////////////////////

struct ValueDeserializer<'de> {
    value: &'de Value,
}

/// TODO: Add docstring.
pub fn from_value<T>(value: &Value) -> Result<T, Error>
where
    T: for<'de> Deserialize<'de>,
{
    let mut deserializer = ValueDeserializer::from_value(value);
    let deserialized = T::deserialize(&mut deserializer)?;
    Ok(deserialized)
}

impl<'de> ValueDeserializer<'de> {
    fn from_value(value: &'de Value) -> Self {
        ValueDeserializer { value }
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut ValueDeserializer<'de> {
    type Error = Error;

    // Primitives:

    fn deserialize_bool<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(self.value.as_i16().unwrap())
    }

    fn deserialize_i32<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i64<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_f32(self.value.as_f32().unwrap())
    }

    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_string<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    // Options

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Value::Null => self.deserialize_unit(visitor),
            _ => visitor.visit_some(self),
        }
    }

    // Deserializes to an absent value (i.e., a None).
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    // Compound types

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    // A "unit" struct has no fields.
    fn deserialize_unit_struct<V>(self, _name: &'static str, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    // Unsupported types

    fn deserialize_bytes<V>(self, __visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, __visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}

impl<'de> de::MapAccess<'de> for ValueDeserializer<'de> {
    type Error = Error;

    fn next_key_seed<S>(&mut self, _seed: S) -> Result<Option<S::Value>, Self::Error>
    where
        S: de::DeserializeSeed<'de>,
    {
        todo!()
    }

    fn next_value_seed<S>(&mut self, _seed: S) -> Result<S::Value, Self::Error>
    where
        S: de::DeserializeSeed<'de>,
    {
        todo!()
    }
}

// TODO: Move tests to unit_tests.rs later.
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn test_serde() {
        // Test primitives, options, tuples, seqs, and maps:

        let expected_deserialized = ();
        let expected_serialized = Value::Null;
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        let value: () = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);

        let expected_deserialized = 1_i16;
        let expected_serialized = Value::from(1_i16);
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        let value: i16 = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);

        let expected_deserialized = None::<String>;
        let expected_serialized = Value::Null;
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        let value: Option<String> = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);

        let expected_deserialized = 1_f32;
        let expected_serialized = Value::Real(1_f32);
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        let value: f32 = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);

        /*
        let expected_deserialized = (1_i16, 3_u32);
        let expected_serialized = Value::Json(json!([1_i16, 3_u32]));
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        let value: (i16, u32) = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);

        let expected_deserialized = [-11_i32, 20_i32];
        let expected_serialized = Value::Json(json!([-11_i32, 20_i32]));
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        let value: [i32; 2] = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);

        let expected_deserialized = vec![1_f32, 3_f32];
        let expected_serialized = Value::Json(json!([1_f32, 3_f32]));
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        let value: Vec<f32> = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);

        let expected_deserialized: Vec<u64> = vec![];
        let expected_serialized = Value::Json(json!([]));
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        let value: Vec<u64> = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);

        let expected_deserialized: HashSet<u64> = HashSet::new();
        let expected_serialized = Value::Json(json!([]));
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        let value: HashSet<u64> = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);

        let expected_deserialized = [("foo".to_string(), true)]
            .into_iter()
            .collect::<HashMap<_, _>>();
        let expected_serialized = Value::Json(json!({"foo": true}));
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        // let value: HashMap<String, bool> = from_value(&value).unwrap();
        // assert_eq!(value, expected_deserialized);

        // let expected_deserialized = HashMap::<String, u64>::new();
        // let expected_deserialized = Value::Json(json!({}));
        // let value = to_value(&expected_deserialized).unwrap();
        // assert_eq!(value, expected_serialized);
        // let value: HashMap<String, bool> = from_value(&value).unwrap();
        // assert_eq!(value, expected_deserialized);

        // Test more complex types:

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct BasicStruct {
            foo: u64,
        }

        let expected_deserialized = BasicStruct { foo: 1 };
        let expected_serialized = Value::Json(json!({"foo": 1}));
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        let value: BasicStruct = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);
        */
        // TODO: the rest.
    }
}
