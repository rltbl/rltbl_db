//! Implement `serde` for values and rows.

use crate::{Error, JsonValue, Value};
use serde::{Deserialize, Serialize, ser};
use serde_json::{Map as JsonMap, json, value::Serializer as JsonValueSerializer};

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
                .ok_or(Error::SerdeError(format!("Not a string: {key}")))?
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

// TODO: Implement deserialization.

// TODO: Move tests to unit_tests.rs later.
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn test_serde() {
        // Test primitives, options, tuples, seqs, and maps:
        let value = to_value(&()).unwrap();
        assert_eq!(value, Value::Null);

        let value = to_value(&1_i16).unwrap();
        assert_eq!(value, Value::SmallInteger(1));

        let value = to_value(&None::<String>).unwrap();
        assert_eq!(value, Value::Null);

        let value = to_value(&Some(1_f32)).unwrap();
        assert_eq!(value, Value::Real(1_f32));

        let value = to_value(&(1_i16, 3_u32)).unwrap();
        assert_eq!(value, Value::Json(json!([1_i16, 3_u32])));

        let value = to_value(&[-11_i32, 20_i32]).unwrap();
        assert_eq!(value, Value::Json(json!([-11_i32, 20_i32])));

        let value = to_value(&vec![1_f32, 3_f32]).unwrap();
        assert_eq!(value, Value::Json(json!([1_f32, 3_f32])));

        let value = to_value::<Vec<u64>>(&vec![]).unwrap();
        assert_eq!(value, Value::Json(json!([])));

        let value = to_value::<HashSet<f64>>(&HashSet::new()).unwrap();
        assert_eq!(value, Value::Json(json!([])));

        let mut input = HashMap::new();
        input.insert("foo".to_string(), true);
        let value = to_value(&input).unwrap();
        assert_eq!(value, Value::Json(json!({"foo": true})));

        let input = HashMap::<String, u64>::new();
        let value = to_value(&input).unwrap();
        assert_eq!(value, Value::Json(json!({})));

        // Test more complex types:

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct BasicStruct {
            foo: u64,
        }

        let expected_struct = BasicStruct { foo: 1 };
        let serialized = to_value(&expected_struct).unwrap();
        assert_eq!(serialized, Value::Json(json!({"foo": 1})),);

        // TODO: Start on deserialization first, then go on to the rest:

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct NormalStruct {
            foo: String,
            bar: u64,
            list: Vec<i16>,
            tuple: (u64, String),
            json: JsonValue,
        }

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct UnitStruct;

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct NewTypeStruct(i64);

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct TupleStruct(i64, i64);

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        enum Enumeration {
            UnitVariant,
            NewTypeVariant(f32),
            StructVariant(BasicStruct),
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
    }
}
