//! Implement serialization/deserialization to/from a [Value].

use crate::{Error, JsonValue, Row, Value};
use serde::{
    Deserialize, Deserializer, Serialize, Serializer,
    de::{
        IntoDeserializer, Visitor,
        value::{MapDeserializer, StringDeserializer},
    },
    forward_to_deserialize_any,
    ser::{
        Impossible, SerializeMap, SerializeSeq, SerializeStruct, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
};
use serde_json::{
    json,
    value::{Map as JsonMap, Serializer as JsonSerializer},
};

struct ValueSerializer;

impl Serializer for ValueSerializer {
    type Ok = Value;

    type Error = Error;

    type SerializeSeq = JsonArraySerializer;
    type SerializeTuple = JsonArraySerializer;
    type SerializeTupleStruct = JsonArraySerializer;
    type SerializeTupleVariant = JsonArraySerializer;
    type SerializeMap = JsonObjectSerializer;
    type SerializeStruct = JsonObjectSerializer;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Boolean(v))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(v.into())
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(Value::SmallInteger(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Integer(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::BigInteger(v))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(v.into())
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(v.into())
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(v.into())
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(v.into())
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Real(v))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(Value::BigReal(v))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Text(v.to_string()))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Text(v.to_string()))
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        return Err(Error::SerdeError(
            "Serializing bytes is not supported".to_string(),
        ));
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_unit()
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Null)
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Text(name.to_string()))
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Text(variant.to_string()))
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let json_serializer = JsonSerializer;
        let json_value = value
            .serialize(json_serializer)
            .map_err(|err| Error::SerdeError(err.to_string()))?;
        let json_value = json!({variant.to_string(): json_value});
        Ok(Value::Json(json_value))
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        Ok(JsonArraySerializer::new())
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        Ok(JsonArraySerializer::new())
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        Ok(JsonArraySerializer::new())
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        Ok(JsonArraySerializer {
            items: Vec::new(),
            name: Some(variant.to_string()),
        })
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(JsonObjectSerializer::new())
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        return Err(Error::SerdeError(
            "Serializing struct variant is not supported".to_string(),
        ));
    }
}

struct JsonArraySerializer {
    items: Vec<JsonValue>,
    name: Option<String>,
}

impl JsonArraySerializer {
    fn new() -> Self {
        JsonArraySerializer {
            items: Vec::new(),
            name: None,
        }
    }
}

impl SerializeSeq for JsonArraySerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.items.push(json!(value));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Json(json!(self.items)))
    }
}

impl SerializeTuple for JsonArraySerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.items.push(json!(value));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Json(json!(self.items)))
    }
}

impl SerializeTupleStruct for JsonArraySerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.items.push(json!(value));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Json(json!(self.items)))
    }
}

impl SerializeTupleVariant for JsonArraySerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.items.push(json!(value));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Json(
            json!({self.name.expect("TupleVariant name"): self.items}),
        ))
    }
}

struct JsonObjectSerializer {
    map: serde_json::Map<String, JsonValue>,
    next_key: Option<String>,
}

impl JsonObjectSerializer {
    fn new() -> Self {
        JsonObjectSerializer {
            map: JsonMap::new(),
            next_key: None,
        }
    }
}

impl SerializeMap for JsonObjectSerializer {
    type Ok = Value;
    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        let k = json!(key);
        let k = match k {
            serde_json::Value::String(k) => k,
            _ => k.to_string(),
        };
        self.next_key = Some(k);
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Error>
    where
        T: ?Sized + Serialize,
    {
        let key = self
            .next_key
            .clone()
            .expect("serialize_value called before serialize_key");
        self.next_key = None;
        self.map.insert(key, json!(value));
        Ok(())
    }

    fn end(self) -> Result<Value, Error> {
        Ok(Value::Json(JsonValue::Object(self.map)))
    }
}

impl SerializeStruct for JsonObjectSerializer {
    type Ok = Value;

    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.map.insert(key.to_string(), json!(value));
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(Value::Json(JsonValue::Object(self.map)))
    }
}

impl<'de> Deserializer<'de> for Value {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // println!("deserialize_any {self:?}");
        match self {
            Value::Null => visitor.visit_none(),
            Value::Boolean(v) => visitor.visit_bool(v),
            Value::SmallInteger(v) => visitor.visit_i16(v),
            Value::Integer(v) => visitor.visit_i32(v),
            Value::BigInteger(v) => visitor.visit_i64(v),
            Value::Real(v) => visitor.visit_f32(v),
            Value::BigReal(v) => visitor.visit_f64(v),
            Value::Numeric(_) => todo!(),
            Value::Text(v) => visitor.visit_string(v),
            Value::Json(value) => value
                .deserialize_any(visitor)
                .map_err(|e| Error::SerdeError(format!("Could not deserialize JSON value: {e}"))),
            Value::Other(_, _, _) => todo!(),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self {
            Value::Null => visitor.visit_none(),
            _ => visitor.visit_some(self),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_unit()
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // println!("deserialize_enum {self:?}");
        match self {
            Value::Text(text) => match text.starts_with("{") {
                true => {
                    let json_value: JsonValue = serde_json::from_str(&text)?;
                    Ok(json_value.deserialize_enum(name, variants, visitor)?)
                }
                false => visitor.visit_enum(StringDeserializer::new(text)),
            },
            Value::Json(value) => Ok(value.deserialize_enum(name, variants, visitor)?),
            _ => Err(Error::SerdeError(format!(
                "Invalid data for enum: {self:?}"
            ))),
        }
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // println!("deserialize_seq");
        match self {
            Value::Text(text) => {
                let json_value: JsonValue = serde_json::from_str(&text)?;
                Ok(json_value.deserialize_seq(visitor)?)
            }
            Value::Json(value) => Ok(value.deserialize_seq(visitor)?),
            _ => Err(Error::SerdeError(format!(
                "Invalid data for enum: {self:?}"
            ))),
        }
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // println!("deserialize_tuple_struct {name} {len}");
        match self {
            Value::Json(value) => Ok(value.deserialize_tuple_struct(name, len, visitor)?),
            _ => Err(Error::SerdeError(format!(
                "Invalid data for tuple struct: {self:?}"
            ))),
        }
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // println!("deserialize_struct");
        match self {
            Value::Text(text) => {
                let json_value: JsonValue = serde_json::from_str(&text)?;
                Ok(json_value.deserialize_struct(name, fields, visitor)?)
            }
            Value::Json(value) => Ok(value.deserialize_struct(name, fields, visitor)?),
            _ => Err(Error::SerdeError(format!(
                "Invalid data for enum: {self:?}"
            ))),
        }
    }

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit_struct tuple
        map identifier ignored_any
    }
}

impl<'de> IntoDeserializer<'de, Error> for Value {
    type Deserializer = Self;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

/// Serialize the given item into a [Value].
pub fn to_value<T>(item: &T) -> Result<Value, Error>
where
    T: ?Sized + Serialize,
{
    let serializer = ValueSerializer;
    let value = item.serialize(serializer)?;
    Ok(value)
}

/// Deserialize the given value.
pub fn from_value<T>(value: &Value) -> Result<T, Error>
where
    T: for<'de> Deserialize<'de>,
{
    T::deserialize(value.clone()) // TODO: don't clone
}

pub struct RowSerializer {
    row: Row,
}

impl RowSerializer {
    pub fn new() -> Self {
        RowSerializer { row: Row::new() }
    }
}

impl<'a> Serializer for &'a mut RowSerializer {
    type Ok = ();

    type Error = Error;

    type SerializeSeq = Impossible<Self::Ok, Self::Error>;
    type SerializeTuple = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleStruct = Impossible<Self::Ok, Self::Error>;
    type SerializeTupleVariant = Impossible<Self::Ok, Self::Error>;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Impossible<Self::Ok, Self::Error>;

    fn serialize_bool(self, _v: bool) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i8(self, _v: i8) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i16(self, _v: i16) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i32(self, _v: i32) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_i64(self, _v: i64) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u8(self, _v: u8) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u16(self, _v: u16) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u32(self, _v: u32) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_u64(self, _v: u64) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_f32(self, _v: f32) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_f64(self, _v: f64) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_char(self, _v: char) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_str(self, _v: &str) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_bytes(self, _v: &[u8]) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_some<T>(self, _value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_newtype_variant<T>(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        todo!()
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        todo!()
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        todo!()
    }

    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        todo!()
    }

    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        Ok(self)
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.serialize_map(Some(len))
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        _variant: &'static str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        todo!()
    }

    fn serialize_i128(self, v: i128) -> Result<Self::Ok, Self::Error> {
        let _ = v;
        Err(serde::ser::Error::custom("i128 is not supported"))
    }

    fn serialize_u128(self, v: u128) -> Result<Self::Ok, Self::Error> {
        let _ = v;
        Err(serde::ser::Error::custom("u128 is not supported"))
    }

    fn collect_seq<I>(self, _iter: I) -> Result<Self::Ok, Self::Error>
    where
        I: IntoIterator,
        <I as IntoIterator>::Item: Serialize,
    {
        // let mut iter = iter.into_iter();
        // let mut serializer = tri!(self.serialize_seq(iterator_len_hint(&iter)));
        // tri!(iter.try_for_each(|item| serializer.serialize_element(&item)));
        // serializer.end()
        todo!()
    }

    fn collect_map<K, V, I>(self, _iter: I) -> Result<Self::Ok, Self::Error>
    where
        K: Serialize,
        V: Serialize,
        I: IntoIterator<Item = (K, V)>,
    {
        // let mut iter = iter.into_iter();
        // let mut serializer = tri!(self.serialize_map(iterator_len_hint(&iter)));
        // tri!(iter.try_for_each(|(key, value)| serializer.serialize_entry(&key, &value)));
        // serializer.end()
        todo!()
    }

    fn collect_str<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + std::fmt::Display,
    {
        self.serialize_str(&value.to_string())
    }

    fn is_human_readable(&self) -> bool {
        true
    }
}

impl<'a> SerializeMap for &'a mut RowSerializer {
    type Ok = ();

    type Error = Error;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let k = to_value(key)?;
        match k {
            Value::Text(text) => self.row.insert(text, Value::Null),
            _ => return Err(Error::SerdeError(format!("Invalid key for row: {k:?}"))),
        };
        Ok(())
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let serializer = ValueSerializer;
        let v = value.serialize(serializer)?;
        match self.row.last_entry() {
            Some(mut entry) => entry.insert(v),
            None => return Err(Error::SerdeError(format!("Missing key for value: {v:?}"))),
        };
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

impl<'a> SerializeStruct for &'a mut RowSerializer {
    type Ok = ();

    type Error = Error;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let v = value.serialize(ValueSerializer)?;
        self.row.insert(key.to_string(), v);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(())
    }
}

pub fn to_row<T>(item: &T) -> Result<Row, Error>
where
    T: Serialize,
{
    let mut serializer = RowSerializer { row: Row::new() };
    item.serialize(&mut serializer)?;
    Ok(serializer.row)
}

pub fn from_row<T>(row: &Row) -> Result<T, Error>
where
    T: for<'de> Deserialize<'de>,
{
    T::deserialize(MapDeserializer::new(row.clone().map.into_iter())) // TODO: don't clone?
}
