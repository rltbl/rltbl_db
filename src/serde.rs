use crate::{
    core::DbError,
    db_value::{DbRow, DbValue, JsonRow, JsonValue},
};
use rust_decimal::prelude::ToPrimitive;
use serde::{Deserialize, Serialize, ser};

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
    T: for<'a> Deserialize<'a>,
{
    // TODO: Can we do this directly without the intermediate step of first converting to JSON?
    // The method below works and does not throw away information, so (unlike the case of
    // serialization) we do not *need* to find an alternative method, but this seems inefficient
    // and it would be better to deserialize directly from a DbRow to a T struct if possible.
    let mut flat_json_row = JsonRow::new();
    for (column, value) in db_row.iter() {
        match value {
            DbValue::Null => flat_json_row.insert(column.to_string(), JsonValue::Null),
            DbValue::Boolean(num) => {
                flat_json_row.insert(column.to_string(), JsonValue::from(*num))
            }
            DbValue::SmallInteger(num) => {
                flat_json_row.insert(column.to_string(), JsonValue::from(*num))
            }
            DbValue::Integer(num) => {
                flat_json_row.insert(column.to_string(), JsonValue::from(*num))
            }
            DbValue::BigInteger(num) => {
                flat_json_row.insert(column.to_string(), JsonValue::from(*num))
            }
            DbValue::Real(num) => flat_json_row.insert(column.to_string(), JsonValue::from(*num)),
            DbValue::BigReal(num) => {
                flat_json_row.insert(column.to_string(), JsonValue::from(*num))
            }
            DbValue::Numeric(num) => {
                flat_json_row.insert(column.to_string(), JsonValue::from(num.to_f64()))
            }
            DbValue::Text(txt) => {
                flat_json_row.insert(column.to_string(), JsonValue::from(txt.to_string()))
            }
        };
    }
    let flat_string_row = format!("{}", serde_json::json!(flat_json_row));
    let t_struct: T = serde_json::from_str(&flat_string_row).unwrap();
    Ok(t_struct)
}

// TODO: Maybe this doesn't need to be public.
// TODO: Rename this to `Serializer` (or at least give it a better name).
#[derive(Debug)]
pub struct MySerializer {
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
        todo!()
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
        todo!()
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
        todo!()
    }

    fn serialize_str(self, value: &str) -> Result<(), Self::Error> {
        self.values.push(DbValue::from(value));
        Ok(())
    }

    fn serialize_bytes(self, _values: &[u8]) -> Result<(), Self::Error> {
        todo!()
    }

    fn serialize_none(self) -> Result<(), Self::Error> {
        todo!()
    }

    fn serialize_some<T>(self, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_unit(self) -> Result<(), Self::Error> {
        todo!()
    }

    fn serialize_unit_struct(self, _name: &str) -> Result<(), Self::Error> {
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

    fn serialize_newtype_struct<T>(self, _name: &str, _value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
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

    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        todo!()
    }

    fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        todo!()
    }

    fn serialize_tuple_struct(
        self,
        _name: &str,
        _len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
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
        todo!()
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
        todo!()
    }

    // Close the sequence.
    fn end(self) -> Result<(), Self::Error> {
        todo!()
    }
}

impl<'a> ser::SerializeTuple for &'a mut MySerializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_element<T>(&mut self, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<(), DbError> {
        todo!()
    }
}

impl<'a> ser::SerializeTupleStruct for &'a mut MySerializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<(), DbError> {
        todo!()
    }
}

impl<'a> ser::SerializeTupleVariant for &'a mut MySerializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<(), DbError> {
        todo!()
    }
}

impl<'a> ser::SerializeMap for &'a mut MySerializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_key<T>(&mut self, _key: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_value<T>(&mut self, _value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<(), DbError> {
        todo!()
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
        todo!()
    }

    fn end(self) -> Result<(), DbError> {
        todo!()
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

        // // Serializing or deserializing a DbRow into a DbRow should yield a DbRow that is
        // // identical to the original one. TODO: This is not yet working.
        // let db_row = db_row! {"value".into() => DbValue::from("foo")};
        // assert_eq!(db_row, to_db_row(&db_row).unwrap());
        // assert_eq!(db_row, from_db_row(&db_row).unwrap());
    }
}
