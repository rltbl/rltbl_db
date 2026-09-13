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

/// TODO: Add docstring.
pub fn to_value<T>(value: &T) -> Result<Value, Error>
where
    T: Serialize,
{
    // Serialize the given value.
    let mut serializer = ValueSerializer::default();
    value.serialize(&mut serializer)?;
    Ok(serializer.value)
}

#[derive(Default)]
struct ValueSerializer {
    value: Value,
    inner_value: JsonValue,
    remaining_fields: usize,
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

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        if self.inner_value == JsonValue::Null {
            self.remaining_fields = len;
            // Start a new empty inner value which will be progressively filled in later:
            self.inner_value = json!([]);
            Ok(self)
        } else {
            Ok(self)
        }
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        // TODO: think about this. When would len be None? I guess this would be the case
        // if the seq were an iterator, but I don't think we are going to support that.
        let len = match len {
            None => {
                return Err(Error::SerdeError("Sequence length unknown".to_string()));
            }
            Some(len) => len,
        };
        if self.inner_value == JsonValue::Null {
            self.remaining_fields = len;
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
        if self.inner_value == JsonValue::Null {
            self.remaining_fields = len;
            // Start a new empty inner value which will be progressively filled in later:
            self.inner_value = json!({
                "keys": [],
                "values": [],
            });
            Ok(self)
        } else {
            self.serialize_map(Some(len))
        }
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
        variant: &str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let json_serializer = JsonValueSerializer;
        let json_value = value
            .serialize(json_serializer)
            .map_err(|err| Error::SerdeError(err.to_string()))?;
        let json_value = json!({variant.to_string(): json_value});
        self.value = Value::Json(json_value);
        Ok(())
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

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        // Add the key to the keys list:
        match self
            .inner_value
            .get_mut("keys")
            .and_then(|k| k.as_array_mut())
        {
            Some(inner) => inner.push(json!(key)),
            None => {
                return Err(Error::SerdeError(format!(
                    "SchmKeyInvalid inner value: {:?}",
                    self.inner_value
                )));
            }
        };
        // Add the value to the values list:
        match self
            .inner_value
            .get_mut("values")
            .and_then(|v| v.as_array_mut())
        {
            Some(inner) => inner.push(json!(value)),
            None => {
                return Err(Error::SerdeError(format!(
                    "SchmValueInvalid inner value: {:?}",
                    self.inner_value
                )));
            }
        };
        Ok(())
    }

    fn end(self) -> Result<(), Error> {
        if self.inner_value != JsonValue::Null {
            let empty_vec = vec![];
            let keys = self
                .inner_value
                .get("keys")
                .and_then(|k| k.as_array())
                .unwrap_or(&empty_vec)
                .into_iter()
                .collect::<Vec<_>>();
            let values = self
                .inner_value
                .get("values")
                .and_then(|v| v.as_array())
                .unwrap_or(&empty_vec)
                .into_iter()
                .collect::<Vec<_>>();
            if keys.len() != values.len() {
                // TODO: Use a proper error here.
                panic!();
            }

            let mut json_map = JsonMap::new();
            for i in 0..keys.len() {
                json_map.insert(keys[i].as_str().unwrap().to_string(), values[i].clone());
            }

            self.value = Value::Json(json!(json_map));
            self.inner_value = JsonValue::Null;
        }
        Ok(())
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

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        if self.remaining_fields > 0 {
            self.remaining_fields -= 1;
            let json_serializer = JsonValueSerializer;
            let json_value = value
                .serialize(json_serializer)
                .map_err(|err| Error::SerdeError(err.to_string()))?;
            let inner = match self.inner_value.as_array_mut() {
                Some(inner) => inner,
                None => {
                    return Err(Error::SerdeError(format!(
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

    fn end(self) -> Result<(), Error> {
        if self.inner_value != JsonValue::Null {
            self.value = Value::Json(self.inner_value.clone());
            self.inner_value = JsonValue::Null;
        }
        Ok(())
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

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        if self.remaining_fields > 0 {
            self.remaining_fields -= 1;
            let json_serializer = JsonValueSerializer;
            let json_value = value
                .serialize(json_serializer)
                .map_err(|err| Error::SerdeError(err.to_string()))?;
            let inner = match self.inner_value.as_array_mut() {
                Some(inner) => inner,
                None => {
                    return Err(Error::SerdeError(format!(
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
            self.value = Value::Json(self.inner_value.clone());
            self.inner_value = JsonValue::Null;
        }
        Ok(())
    }
}

// This might be called when serializing a JSON value that happens to be a map.
impl<'a> ser::SerializeMap for &'a mut ValueSerializer {
    // These need to match the `Ok` and `Error` types of ValueSerializer:
    type Ok = ();
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Error>
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
                return Err(Error::SerdeError(format!(
                    "Invalid inner value: {:?}",
                    self.inner_value
                )));
            }
        };
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Error>
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
                return Err(Error::SerdeError(format!(
                    "Invalid inner value: {:?}",
                    self.inner_value
                )));
            }
        };
        Ok(())
    }

    fn end(self) -> Result<(), Error> {
        let empty_vec = vec![];
        let keys = {
            if self.inner_value == JsonValue::Null {
                &empty_vec
            } else {
                match self.inner_value.get("keys").and_then(|k| k.as_array()) {
                    Some(inner) => inner,
                    None => {
                        return Err(Error::SerdeError(format!(
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
                        return Err(Error::SerdeError(format!(
                            "Invalid inner value: {:?}",
                            self.inner_value
                        )));
                    }
                }
            }
        };
        if keys.len() != values.len() {
            return Err(Error::SerdeError(format!(
                "Inner keys and values have different lengths: \
                 Keys: {keys:?} (length: {klen}), \
                 Values: {values:?} (length: {vlen})",
                klen = keys.len(),
                vlen = values.len(),
            )));
        }
        let mut json_map: JsonMap<String, JsonValue> = JsonMap::new();
        for (i, key) in keys.iter().enumerate() {
            let key = key
                .as_str()
                .ok_or(Error::SerdeError(format!("Not a string 1: {key}")))?
                .to_string();
            json_map.insert(key, values[i].clone());
        }
        self.value = Value::Json(json!(json_map));
        self.inner_value = JsonValue::Null;
        Ok(())
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

/// TODO: Add docstring.
pub fn from_value<T>(value: &Value) -> Result<T, Error>
where
    T: for<'de> Deserialize<'de>,
{
    let mut deserializer = ValueDeserializer::from_value(value);
    let deserialized = T::deserialize(&mut deserializer)?;
    Ok(deserialized)
}

#[derive(Debug)]
pub(crate) struct ValueDeserializer<'de> {
    first: bool,
    value: &'de Value,
    inner_keys: Vec<&'de str>,
    inner_values: Vec<&'de JsonValue>,
}

impl<'de> ValueDeserializer<'de> {
    pub(crate) fn from_value(input: &'de Value) -> Self {
        let mut inner_keys = vec![];
        let mut inner_values = vec![];
        match input {
            Value::Json(JsonValue::Object(obj)) => {
                inner_keys = obj.keys().map(|s| s.as_str()).collect::<Vec<_>>();
                inner_values = obj.values().collect::<Vec<_>>();
            }
            _ => (),
        };
        ValueDeserializer {
            first: true,
            value: input,
            inner_keys,
            inner_values,
        }
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
        if !self.inner_values.is_empty() {
            let value = self.inner_values.pop().unwrap();
            match value {
                JsonValue::Null => self.deserialize_unit(visitor),
                _ => {
                    let value = Value::from(value)
                        .as_i16()
                        .ok_or(Error::SerdeError(format!("Not an i16: {}", value)))?;
                    visitor.visit_i16(value)
                }
            }
        } else {
            match self.value {
                Value::Null => self.deserialize_unit(visitor),
                _ => {
                    let value = self
                        .value
                        .as_i16()
                        .ok_or(Error::SerdeError(format!("Not an i16: {}", self.value)))?;
                    visitor.visit_i16(value)
                }
            }
        }
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

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        if !self.inner_values.is_empty() {
            let value = self.inner_values.pop().unwrap();
            match value {
                JsonValue::Null => self.deserialize_unit(visitor),
                _ => {
                    let value = value
                        .as_u64()
                        .ok_or(Error::SerdeError(format!("Not an u64: {}", value)))?;
                    visitor.visit_u64(value)
                }
            }
        } else {
            match self.value {
                Value::Null => self.deserialize_unit(visitor),
                _ => {
                    let value = self
                        .value
                        .as_u64()
                        .ok_or(Error::SerdeError(format!("Not an u64: {}", self.value)))?;
                    visitor.visit_u64(value)
                }
            }
        }
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        if !self.inner_values.is_empty() {
            let value = self.inner_values.pop().unwrap();
            match value {
                JsonValue::Null => self.deserialize_unit(visitor),
                _ => {
                    let value = Value::from(value)
                        .as_f32()
                        .ok_or(Error::SerdeError(format!("Not an f32: {}", value)))?;
                    visitor.visit_f32(value)
                }
            }
        } else {
            match self.value {
                Value::Null => self.deserialize_unit(visitor),
                _ => {
                    let value = self
                        .value
                        .as_f32()
                        .ok_or(Error::SerdeError(format!("Not an f32: {}", self.value)))?;
                    visitor.visit_f32(value)
                }
            }
        }
    }

    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        if !self.inner_values.is_empty() {
            let value = self.inner_values.pop().unwrap();
            match value {
                JsonValue::Null => self.deserialize_unit(visitor),
                _ => {
                    let value = value
                        .as_str()
                        .ok_or(Error::SerdeError(format!("Not an str: {}", value)))?;
                    visitor.visit_str(value)
                }
            }
        } else {
            match self.value {
                Value::Null => self.deserialize_unit(visitor),
                _ => {
                    let value = self
                        .value
                        .as_str()
                        .ok_or(Error::SerdeError(format!("Not a string 2: {}", self.value)))?;
                    visitor.visit_borrowed_str(value)
                }
            }
        }
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
        if *self.value != Value::Null {
            return Err(Error::SerdeError("Expected NULL".to_string()));
        }
        visitor.visit_unit()
    }

    // Compound types

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        if self.first {
            self.first = false;
            self.deserialize_map(visitor)
        } else {
            match self.value {
                Value::Text(value) | Value::Json(JsonValue::String(value)) => {
                    serde_json::Deserializer::from_str(value)
                        .deserialize_struct(name, fields, visitor)
                        .map_err(|err| Error::SerdeError(err.to_string()))
                }
                Value::Json(value) => match value {
                    JsonValue::Array(_) => value.deserialize_map(visitor).map_err(|err| {
                        Error::SerdeError(format!("Error deserializing array: '{err}'."))
                    }),
                    JsonValue::Object(_) => value.deserialize_map(visitor).map_err(|err| {
                        Error::SerdeError(format!("Error deserializing object: '{err}'."))
                    }),
                    _ => {
                        return Err(Error::SerdeError(format!(
                            "In deserialize_struct(): Invalid JSON value: {:?}",
                            self.value
                        )));
                    }
                },
                _ => {
                    return Err(Error::SerdeError(format!(
                        "Invalid DB value: {:?}",
                        self.value
                    )));
                }
            }
        }
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        let key = self
            .inner_keys
            .pop()
            .ok_or(Error::SerdeError("No more keys".to_string()))
            .unwrap();
        visitor.visit_borrowed_str(key)
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

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Value::Text(value) | Value::Json(JsonValue::String(value)) => {
                serde_json::Deserializer::from_str(value)
                    .deserialize_seq(visitor)
                    .map_err(|err| Error::SerdeError(err.to_string()))
            }
            Value::Json(value) => match value {
                JsonValue::Array(_) => value.deserialize_seq(visitor).map_err(|err| {
                    Error::SerdeError(format!("Error deserializing array: '{err}'."))
                }),
                JsonValue::Object(_) => value.deserialize_seq(visitor).map_err(|err| {
                    Error::SerdeError(format!("Error deserializing object: '{err}'."))
                }),
                _ => {
                    return Err(Error::SerdeError(format!(
                        "In deserialize_seq(): Invalid JSON value: {:?}",
                        self.value
                    )));
                }
            },
            _ => {
                return Err(Error::SerdeError(format!(
                    "Invalid DB value: {:?}",
                    self.value
                )));
            }
        }
    }

    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, Error>
    where
        V: Visitor<'de>,
    {
        match self.value {
            Value::Text(value) | Value::Json(JsonValue::String(value)) => {
                serde_json::Deserializer::from_str(value)
                    .deserialize_seq(visitor)
                    .map_err(|err| Error::SerdeError(err.to_string()))
            }
            Value::Json(value) => match value {
                JsonValue::Array(_) => value.deserialize_seq(visitor).map_err(|err| {
                    Error::SerdeError(format!("Error deserializing array: '{err}'."))
                }),
                JsonValue::Object(_) => value.deserialize_seq(visitor).map_err(|err| {
                    Error::SerdeError(format!("Error deserializing object: '{err}'."))
                }),
                _ => {
                    return Err(Error::SerdeError(format!(
                        "In deserialize_tuple(): Invalid JSON value: {:?}",
                        self.value
                    )));
                }
            },
            _ => {
                return Err(Error::SerdeError(format!(
                    "Invalid DB value: {:?}",
                    self.value
                )));
            }
        }
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

    fn next_key_seed<S>(&mut self, seed: S) -> Result<Option<S::Value>, Self::Error>
    where
        S: de::DeserializeSeed<'de>,
    {
        if self.inner_keys.len() == 0 {
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

        // TODO: the rest.
    }
}
