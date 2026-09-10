//! Implement `serde` for values and rows.

use crate::{Error, Value};
use serde::{Deserialize, Serialize, ser};

/// TODO: Add docstring.
pub fn to_value<T>(value: &T) -> Result<Value, Error>
where
    T: Serialize,
{
    // Serialize the given value.
    let mut serializer = ValueSerializer::new();
    value.serialize(&mut serializer)?;
    Ok(serializer.value)
}

/// TODO: Add docstring.
pub fn from_value<T>(_value: &Value) -> Result<T, Error>
where
    T: for<'de> Deserialize<'de>,
{
    todo!()
}

////////////////////////////////////////////////////////////////////////////////
// Serialization implementations
////////////////////////////////////////////////////////////////////////////////

struct ValueSerializer {
    value: Value,
}

impl ValueSerializer {
    fn new() -> Self {
        ValueSerializer {
            value: Value::default(),
        }
    }
}

impl<'a> ser::Serializer for &'a mut ValueSerializer {
    // The output type produced by this serializer during successful serialization.
    type Ok = ();

    // The error type when some error occurs during serialization.
    type Error = Error;

    // Associated types for keeping track of additional state while serializing compound data
    // structures like sequences and maps. In this case no additional state is required beyond
    // what is already stored in the ValueSerializer struct.
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    // Primitive types
    fn serialize_bool(self, value: bool) -> Result<(), Self::Error> {
        self.value = Value::from(value);
        Ok(())
    }

    fn serialize_i8(self, value: i8) -> Result<(), Self::Error> {
        self.value = Value::from(value);
        Ok(())
    }

    fn serialize_i16(self, value: i16) -> Result<(), Self::Error> {
        self.value = Value::from(value);
        Ok(())
    }

    fn serialize_i32(self, value: i32) -> Result<(), Self::Error> {
        self.value = Value::from(value);
        Ok(())
    }

    fn serialize_i64(self, value: i64) -> Result<(), Self::Error> {
        self.value = Value::from(value);
        Ok(())
    }

    fn serialize_u8(self, value: u8) -> Result<(), Self::Error> {
        self.value = Value::from(value);
        Ok(())
    }

    fn serialize_u16(self, value: u16) -> Result<(), Self::Error> {
        self.value = Value::from(value);
        Ok(())
    }

    fn serialize_u32(self, value: u32) -> Result<(), Self::Error> {
        self.value = Value::from(value);
        Ok(())
    }

    fn serialize_u64(self, value: u64) -> Result<(), Self::Error> {
        self.value = Value::from(value);
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> Result<(), Self::Error> {
        self.value = Value::from(value);
        Ok(())
    }

    fn serialize_f64(self, value: f64) -> Result<(), Self::Error> {
        self.value = Value::from(value);
        Ok(())
    }

    fn serialize_str(self, value: &str) -> Result<(), Self::Error> {
        self.value = Value::from(value);
        Ok(())
    }

    fn serialize_char(self, value: char) -> Result<(), Self::Error> {
        self.value = Value::from(value.to_string());
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

    // Serializes an absent _value (None)
    fn serialize_unit(self) -> Result<(), Self::Error> {
        self.value = Value::Null;
        Ok(())
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
    fn serialize_unit_struct(self, _name: &str) -> Result<(), Self::Error> {
        todo!()
    }

    fn serialize_newtype_struct<T>(self, _name: &str, _value: &T) -> Result<(), Self::Error>
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
    ) -> Result<(), Self::Error> {
        todo!()
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
    fn serialize_bytes(self, _values: &[u8]) -> Result<(), Self::Error> {
        todo!()
    }
}

impl<'a> ser::SerializeStruct for &'a mut ValueSerializer {
    // These need to match the `Ok` and `Error` types of ValueSerializer:
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<(), Error> {
        todo!()
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut ValueSerializer {
    // These need to match the `Ok` and `Error` types of ValueSerializer:
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<(), Error> {
        todo!()
    }
}

impl<'a> ser::SerializeTuple for &'a mut ValueSerializer {
    // These need to match the `Ok` and `Error` types of ValueSerializer:
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<(), Error> {
        todo!()
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut ValueSerializer {
    // These need to match the `Ok` and `Error` types of ValueSerializer:
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<(), Error> {
        todo!()
    }
}

impl<'a> ser::SerializeSeq for &'a mut ValueSerializer {
    // These need to match the `Ok` and `Error` types of ValueSerializer:
    type Ok = ();
    type Error = Error;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<(), Self::Error> {
        todo!()
    }
}

// This might be called when serializing a JSON value that happens to be a map.
impl<'a> ser::SerializeMap for &'a mut ValueSerializer {
    // These need to match the `Ok` and `Error` types of ValueSerializer:
    type Ok = ();
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

    fn end(self) -> Result<(), Error> {
        todo!()
    }
}

// Unsupported implementations

impl<'a> ser::SerializeStructVariant for &'a mut ValueSerializer {
    // These need to match the `Ok` and `Error` types of ValueSerializer:
    type Ok = ();
    type Error = Error;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<(), Error> {
        todo!()
    }
}

////////////////////////////////////////////////////////////////////////////////
// Deserialization implementations
////////////////////////////////////////////////////////////////////////////////

// TODO: Implement deserialization.

// TODO: Move tests to unit_tests.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde() {
        let value = to_value(&1_i16).unwrap();
        assert_eq!(value, Value::SmallInteger(1));

        let value = to_value(&None::<String>).unwrap();
        assert_eq!(value, Value::Null);

        let value = to_value(&Some(1_f32)).unwrap();
        assert_eq!(value, Value::Real(1_f32));
    }
}

