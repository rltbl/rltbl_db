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

pub fn to_db_row<T>(value: &T) -> Result<DbRow, DbError>
where
    T: Serialize,
{
    let mut serializer = MySerializer {
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

pub fn from_db_row<T>(db_row: &DbRow) -> Result<T, DbError>
where
    T: for<'de> Deserialize<'de>,
{
    let mut deserializer = MyDeserializer::from_db_row(db_row);
    let t = T::deserialize(&mut deserializer)?;
    if deserializer.keys.is_empty() && deserializer.values.is_empty() {
        Ok(t)
    } else {
        Err(DbError::SerdeError(
            "Deserialization error: Leftover input".to_string(),
        ))
    }
}

// TODO: Remove this function before merging this branch. Keeping it around for now for comparison.
pub fn from_db_row_indirect<T>(db_row: &DbRow) -> Result<T, DbError>
where
    T: for<'a> Deserialize<'a>,
{
    // The method below *works* and, unlike for serialization, where converting from JSON would
    // necessarily throw away type information, converting to JSON in this case does not throw
    // any type information away. So we do not *need* to find an alternative method. However this
    // method (using the intermediate step of deserializing to JSON) seems inefficient
    // and it is probably be better to deserialize directly from a DbRow to a T struct, as is done
    // in from_db_row().
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

// TODO: Rename this to `Serializer` (or at least give it a better name).
#[derive(Debug)]
struct MySerializer {
    keys: Vec<String>,
    values: Vec<DbValue>,
}

impl<'a> ser::Serializer for &'a mut MySerializer {
    // The output type produced by this `MySerializer` during successful
    // serialization.
    type Ok = ();

    // The error type when some error occurs during serialization.
    type Error = DbError;

    // Associated types for keeping track of additional state while serializing
    // compound data structures like sequences and maps. In this case no
    // additional state is required beyond what is already stored in the
    // MySerializer struct.
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
        return Err(DbError::SerdeError(
            "Serializing None is not supported".to_string(),
        ));
    }

    fn serialize_some<T>(self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "Serializing Some is not supported".to_string(),
        ));
    }

    fn serialize_unit(self) -> Result<(), Self::Error> {
        return Err(DbError::SerdeError(
            "Serializing unit is not supported".to_string(),
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

    // Maps are represented in JSON as `{ K: V, K: V, ... }`.
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

impl<'a> ser::SerializeSeq for &'a mut MySerializer {
    // Must match the `Ok` type of the serializer.
    type Ok = ();
    // Must match the `Error` type of the serializer.
    type Error = DbError;

    // Serialize a single element of the sequence.
    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeSeq::serialize_element() is not supported for MySerializer".to_string(),
        ));
    }

    // Close the sequence.
    fn end(self) -> Result<(), Self::Error> {
        return Err(DbError::SerdeError(
            "SerializeSeq::end() is not supported for MySerializer".to_string(),
        ));
    }
}

impl<'a> ser::SerializeTuple for &'a mut MySerializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeTuple::serialize_element() is not supported for MySerializer".to_string(),
        ));
    }

    fn end(self) -> Result<(), DbError> {
        return Err(DbError::SerdeError(
            "SerializeTuple::end() is not supported for MySerializer".to_string(),
        ));
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut MySerializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeTupleStruct::serialize_field() is not supported for MySerializer".to_string(),
        ));
    }

    fn end(self) -> Result<(), DbError> {
        return Err(DbError::SerdeError(
            "SerializeTupleStruct::end() is not supported for MySerializer".to_string(),
        ));
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut MySerializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeTupleVariant::serialize_field() is not supported for MySerializer"
                .to_string(),
        ));
    }

    fn end(self) -> Result<(), DbError> {
        return Err(DbError::SerdeError(
            "SerializeTupleVariant::end() is not supported for MySerializer".to_string(),
        ));
    }
}

impl<'a> ser::SerializeMap for &'a mut MySerializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_key<T>(&mut self, _key: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeMap::serialize_key() is not supported for MySerializer".to_string(),
        ));
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeMap::serialize_value() is not supported for MySerializer".to_string(),
        ));
    }

    fn end(self) -> Result<(), DbError> {
        return Err(DbError::SerdeError(
            "SerializeMap::end() is not supported for MySerializer".to_string(),
        ));
    }
}

impl<'a> ser::SerializeStruct for &'a mut MySerializer {
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

impl<'a> ser::SerializeStructVariant for &'a mut MySerializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, _key: &'static str, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        return Err(DbError::SerdeError(
            "SerializeStructVariant::serialize_field() is not supported for MySerializer"
                .to_string(),
        ));
    }

    fn end(self) -> Result<(), DbError> {
        return Err(DbError::SerdeError(
            "SerializeStructVariant::end() is not supported for MySerializer".to_string(),
        ));
    }
}

#[derive(Debug)]
pub struct MyDeserializer<'de> {
    keys: Vec<&'de str>,
    values: Vec<&'de DbValue>,
}

impl<'de> MyDeserializer<'de> {
    pub fn from_db_row(input: &'de DbRow) -> Self {
        MyDeserializer {
            keys: input.map.keys().map(|s| s.as_str()).collect::<Vec<_>>(),
            values: input.map.values().collect::<Vec<_>>(),
        }
    }

    pub fn next_key(&mut self) -> Option<&'de str> {
        self.keys.pop()
    }

    pub fn next_value(&mut self) -> Option<&'de DbValue> {
        self.values.pop()
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut MyDeserializer<'de> {
    type Error = DbError;

    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // TODO: Change the unimplemented!() calls to actual errors, like we do for
        // unsupported serializations above.
        // println!("In deserialize_any()");
        unimplemented!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_bool()");
        let value = self.next_value().unwrap().as_bool().unwrap();
        visitor.visit_bool(value)
    }

    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_i8()");
        unimplemented!()
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_i16()");
        let value = self.next_value().unwrap().as_i16().unwrap();
        visitor.visit_i16(value)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_i32()");
        let value = self.next_value().unwrap().as_i32().unwrap();
        visitor.visit_i32(value)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_i64()");
        let value = self.next_value().unwrap().as_i64().unwrap();
        visitor.visit_i64(value)
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_u8()");
        unimplemented!()
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_u16()");
        unimplemented!()
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_u32()");
        unimplemented!()
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_u64()");
        unimplemented!()
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_f32()");
        let value = self.next_value().unwrap().as_f32().unwrap();
        visitor.visit_f32(value)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_f64()");
        let value = self.next_value().unwrap().as_f64().unwrap();
        visitor.visit_f64(value)
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_char()");
        unimplemented!()
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_string()");
        unimplemented!()
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_string()");
        let value = self.next_value().unwrap().as_str().unwrap();
        visitor.visit_borrowed_str(value)
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_bytes()");
        unimplemented!()
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_byte_buf()");
        unimplemented!()
    }

    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_option()");
        unimplemented!()
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_unit()");
        unimplemented!()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_unit_struct()");
        unimplemented!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_newtype_struct()");
        unimplemented!()
    }

    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_seq()");
        unimplemented!()
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_tuple()");
        unimplemented!()
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
        // println!("In deserialize_tuple_struct()");
        unimplemented!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_map()");
        let value = visitor.visit_map(self)?;
        Ok(value)
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
        // println!("In deserialize_struct()");
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
        // println!("In deserialize_enum()");
        unimplemented!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // TODO: Remove unwraps here and elsewhere.
        // println!("In deserialize_identifier()");
        let key = self.next_key().unwrap();
        visitor.visit_borrowed_str(key)
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // println!("In deserialize_ignored_any()");
        unimplemented!()
    }
}

impl<'de> de::MapAccess<'de> for MyDeserializer<'de> {
    type Error = DbError;

    fn next_key_seed<S>(&mut self, seed: S) -> Result<Option<S::Value>, Self::Error>
    where
        S: de::DeserializeSeed<'de>,
    {
        // println!("In next_key_seed() with SELF: {self:?}");
        if self.keys.len() == 0 {
            return Ok(None);
        }
        seed.deserialize(&mut *self).map(Some)
    }

    fn next_value_seed<S>(&mut self, seed: S) -> Result<S::Value, Self::Error>
    where
        S: de::DeserializeSeed<'de>,
    {
        // println!("In next_value_seed() with SELF: {self:?}");
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
            smallint: i16,
            mediumint: i32,
            bigint: i64,
            smallfloat: f32,
            bigfloat: f64,
            // biggerfloat: Decimal,
            text: String,
        }

        let expected_struct = TestStruct {
            boolean: true,
            smallint: 1,
            mediumint: 1,
            bigint: 1,
            smallfloat: 1_f32,
            bigfloat: 1_f64,
            // biggerfloat: dec!(1),
            text: 1.to_string(),
        };

        let expected_db_row = db_row! {
            "boolean".into() => DbValue::from(true),
            "smallint".into() => DbValue::from(1_i16),
            "mediumint".into() => DbValue::from(1_i32),
            "bigint".into() => DbValue::from(1_i64),
            "smallfloat".into() => DbValue::from(1_f32),
            "bigfloat".into() => DbValue::from(1_f64),
            // "biggerfloat".into() => DbValue::from(1_i64),
            "text".into() => DbValue::from("1"),
        };
        assert_eq!(expected_db_row, to_db_row(&expected_struct).unwrap());
        assert_eq!(expected_struct, from_db_row(&expected_db_row).unwrap());
        assert_eq!(
            expected_struct,
            from_db_row_indirect(&expected_db_row).unwrap()
        );
    }
}
