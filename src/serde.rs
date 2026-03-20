use crate::{
    core::DbError,
    db_value::{DbRow, DbValue, JsonRow, JsonValue},
};
use serde::{
    Deserialize, Serialize,
    de::{
        self, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess, VariantAccess,
        Visitor,
    },
    ser::{self, SerializeSeq as _},
};
use std::ops::{AddAssign, MulAssign, Neg};

// TODO: Most of the comments and docstrings below were copied (initially) verbatim from
// https://serde.rs/impl-serializer.html. After we adapt that template to our use case these
// need to be updated.

/// Used for serialization / deserialization of a [DbRow] to/from a struct
pub struct Serializer {
    output_old_remove_me: String,
    _output_new_not_used_yet: DbRow,
}

// By convention, the public API of a Serde serializer is one or more `to_abc`
// functions such as `to_string`, `to_bytes`, or `to_writer` depending on what
// Rust types the serializer is able to produce as output.
//

pub fn to_string<T>(value: &T) -> Result<String, DbError>
where
    T: Serialize,
{
    let mut serializer = Serializer {
        output_old_remove_me: String::new(),
        _output_new_not_used_yet: DbRow::new(),
    };
    value.serialize(&mut serializer)?;
    Ok(serializer.output_old_remove_me)
}

pub fn to_db_row<T>(value: &T) -> Result<DbRow, DbError>
where
    T: Serialize,
{
    // TODO: This is a very inefficient way of serializing a struct to a DbRow, but it works.
    // We need to eliminate the dependency on serde_json and serialize directly to a DbRow
    // rather than first to a string. We might be able to use "type OK = TYPE" (see below)
    // to help with this.

    let value_string = to_string(value)?;
    //println!("VALUE STRING: {value_string}");

    let mut db_row = DbRow::new();
    let json_row = serde_json::from_str::<JsonValue>(&value_string).unwrap();
    let json_row = json_row.as_object().unwrap();
    for (column, value) in json_row.iter() {
        //println!("VALUE: {value:?}");
        match value.as_object() {
            Some(value) => {
                assert_eq!(value.len(), 1);
                let value = value.iter().collect::<Vec<_>>()[0];
                let value_type = value.0;
                let value_value = DbValue::from(value.1.clone());
                match value_type.to_lowercase().as_str() {
                    "null" => db_row.insert(column.to_string(), DbValue::Null),
                    "boolean" => db_row.insert(column.to_string(), DbValue::from(value_value)),
                    "smallinteger" => db_row.insert(column.to_string(), DbValue::from(value_value)),
                    "integer" => db_row.insert(column.to_string(), DbValue::from(value_value)),
                    "biginteger" => db_row.insert(column.to_string(), DbValue::from(value_value)),
                    "real" => db_row.insert(column.to_string(), DbValue::from(value_value)),
                    "bigreal" => db_row.insert(column.to_string(), DbValue::from(value_value)),
                    "numeric" => db_row.insert(column.to_string(), DbValue::from(value_value)),
                    "text" => db_row.insert(column.to_string(), DbValue::from(value_value)),
                    _ => panic!(),
                }
            }
            None => db_row.insert(column.to_string(), DbValue::from(value.clone())),
        }
    }
    Ok(db_row)
}

impl<'a> ser::Serializer for &'a mut Serializer {
    // The output type produced by this `Serializer` during successful
    // serialization. Most serializers that produce text or binary output should
    // set `Ok = ()` and serialize into an `io::Write` or buffer contained
    // within the `Serializer` instance, as happens here. Serializers that build
    // in-memory data structures may be simplified by using `Ok` to propagate
    // the data structure around.
    type Ok = ();

    // The error type when some error occurs during serialization.
    type Error = DbError;

    // Associated types for keeping track of additional state while serializing
    // compound data structures like sequences and maps. In this case no
    // additional state is required beyond what is already stored in the
    // Serializer struct.
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    // Here we go with the simple methods. The following 12 methods receive one
    // of the primitive types of the data model and map it to JSON by appending
    // into the output string.

    fn serialize_bool(self, value: bool) -> Result<(), Self::Error> {
        self.output_old_remove_me += match value {
            true => "true",
            false => "false",
        };
        Ok(())
    }

    // JSON does not distinguish between different sizes of integers, so all
    // signed integers will be serialized the same and all unsigned integers
    // will be serialized the same. Other formats, especially compact binary
    // formats, may need independent logic for the different sizes.

    fn serialize_i8(self, value: i8) -> Result<(), Self::Error> {
        self.serialize_i64(i64::from(value))
    }

    fn serialize_i16(self, value: i16) -> Result<(), Self::Error> {
        self.serialize_i64(i64::from(value))
    }

    fn serialize_i32(self, value: i32) -> Result<(), Self::Error> {
        self.serialize_i64(i64::from(value))
    }

    // Not particularly efficient but this is example code anyway. A more
    // performant approach would be to use the `itoa` crate.
    fn serialize_i64(self, value: i64) -> Result<(), Self::Error> {
        self.output_old_remove_me += &value.to_string();
        Ok(())
    }

    fn serialize_u8(self, value: u8) -> Result<(), Self::Error> {
        self.serialize_u64(u64::from(value))
    }

    fn serialize_u16(self, value: u16) -> Result<(), Self::Error> {
        self.serialize_u64(u64::from(value))
    }

    fn serialize_u32(self, value: u32) -> Result<(), Self::Error> {
        self.serialize_u64(u64::from(value))
    }

    fn serialize_u64(self, value: u64) -> Result<(), Self::Error> {
        self.output_old_remove_me += &value.to_string();
        Ok(())
    }

    fn serialize_f32(self, value: f32) -> Result<(), Self::Error> {
        self.serialize_f64(f64::from(value))
    }

    fn serialize_f64(self, value: f64) -> Result<(), Self::Error> {
        self.output_old_remove_me += &value.to_string();
        Ok(())
    }

    // Serialize a char as a single-character string. Other formats may
    // represent this differently.
    fn serialize_char(self, value: char) -> Result<(), Self::Error> {
        self.serialize_str(&value.to_string())
    }

    // This only works for strings that don't require escape sequences but you
    // get the idea. For example it would emit invalid JSON if the input string
    // contains a '"' character.
    fn serialize_str(self, value: &str) -> Result<(), Self::Error> {
        self.output_old_remove_me += "\"";
        self.output_old_remove_me += value;
        self.output_old_remove_me += "\"";
        Ok(())
    }

    // Serialize a byte array as an array of bytes. Could also use a base64
    // string here. Binary formats will typically represent byte arrays more
    // compactly.
    fn serialize_bytes(self, values: &[u8]) -> Result<(), Self::Error> {
        let mut seq = self.serialize_seq(Some(values.len()))?;
        for byte in values {
            seq.serialize_element(byte)?;
        }
        seq.end()
    }

    // An absent optional is represented as the JSON `null`.
    fn serialize_none(self) -> Result<(), Self::Error> {
        self.serialize_unit()
    }

    // A present optional is represented as just the contained value. Note that
    // this is a lossy representation. For example the values `Some(())` and
    // `None` both serialize as just `null`. Unfortunately this is typically
    // what people expect when working with JSON. Other formats are encouraged
    // to behave more intelligently if possible.
    fn serialize_some<T>(self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    // In Serde, unit means an anonymous value containing no data. Map this to
    // JSON as `null`.
    fn serialize_unit(self) -> Result<(), Self::Error> {
        self.output_old_remove_me += "null";
        Ok(())
    }

    // Unit struct means a named value containing no data. Again, since there is
    // no data, map this to JSON as `null`. There is no need to serialize the
    // name in most formats.
    fn serialize_unit_struct(self, _name: &str) -> Result<(), Self::Error> {
        self.serialize_unit()
    }

    // When serializing a unit variant (or any other kind of variant), formats
    // can choose whether to keep track of it by index or by name. Binary
    // formats typically use the index of the variant and human-readable formats
    // typically use the name.
    fn serialize_unit_variant(
        self,
        _name: &str,
        _variant_index: u32,
        variant: &str,
    ) -> Result<(), Self::Error> {
        self.serialize_str(variant)
    }

    // As is done here, serializers are encouraged to treat newtype structs as
    // insignificant wrappers around the data they contain.
    fn serialize_newtype_struct<T>(self, _name: &str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    // Note that newtype variant (and all of the other variant serialization
    // methods) refer exclusively to the "externally tagged" enum
    // representation.
    //
    // Serialize this to JSON in externally tagged form as `{ NAME: VALUE }`.
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
        self.output_old_remove_me += "{";
        variant.serialize(&mut *self)?;
        self.output_old_remove_me += ":";
        value.serialize(&mut *self)?;
        self.output_old_remove_me += "}";
        Ok(())
    }

    // Now we get to the serialization of compound types.
    //
    // The start of the sequence, each value, and the end are three separate
    // method calls. This one is responsible only for serializing the start,
    // which in JSON is `[`.
    //
    // The length of the sequence may or may not be known ahead of time. This
    // doesn't make a difference in JSON because the length is not represented
    // explicitly in the serialized form. Some serializers may only be able to
    // support sequences for which the length is known up front.
    fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        self.output_old_remove_me += "[";
        Ok(self)
    }

    // Tuples look just like sequences in JSON. Some formats may be able to
    // represent tuples more efficiently by omitting the length, since tuple
    // means that the corresponding `Deserialize implementation will know the
    // length without needing to look at the serialized data.
    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        self.serialize_seq(Some(len))
    }

    // Tuple structs look just like sequences in JSON.
    fn serialize_tuple_struct(
        self,
        _name: &str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        self.serialize_seq(Some(len))
    }

    // Tuple variants are represented in JSON as `{ NAME: [DATA...] }`. Again
    // this method is only responsible for the externally tagged representation.
    fn serialize_tuple_variant(
        self,
        _name: &str,
        _variant_index: u32,
        variant: &str,
        _len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        self.output_old_remove_me += "{";
        variant.serialize(&mut *self)?;
        self.output_old_remove_me += ":[";
        Ok(self)
    }

    // Maps are represented in JSON as `{ K: V, K: V, ... }`.
    fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        self.output_old_remove_me += "{";
        Ok(self)
    }

    // Structs look just like maps in JSON. In particular, JSON requires that we
    // serialize the field names of the struct. Other formats may be able to
    // omit the field names when serializing structs because the corresponding
    // Deserialize implementation is required to know what the keys are without
    // looking at the serialized data.
    fn serialize_struct(
        self,
        _name: &str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        self.serialize_map(Some(len))
    }

    // Struct variants are represented in JSON as `{ NAME: { K: V, ... } }`.
    // This is the externally tagged representation.
    fn serialize_struct_variant(
        self,
        _name: &str,
        _variant_index: u32,
        variant: &str,
        _len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        self.output_old_remove_me += "{";
        variant.serialize(&mut *self)?;
        self.output_old_remove_me += ":{";
        Ok(self)
    }
}

// The following 7 impls deal with the serialization of compound types like
// sequences and maps. Serialization of such types is begun by a Serializer
// method and followed by zero or more calls to serialize individual elements of
// the compound type and one call to end the compound type.
//
// This impl is SerializeSeq so these methods are called after `serialize_seq`
// is called on the Serializer.

impl<'a> ser::SerializeSeq for &'a mut Serializer {
    // Must match the `Ok` type of the serializer.
    type Ok = ();
    // Must match the `Error` type of the serializer.
    type Error = DbError;

    // Serialize a single element of the sequence.
    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        if !self.output_old_remove_me.ends_with('[') {
            self.output_old_remove_me += ",";
        }
        value.serialize(&mut **self)
    }

    // Close the sequence.
    fn end(self) -> Result<(), Self::Error> {
        self.output_old_remove_me += "]";
        Ok(())
    }
}

// Same thing but for tuples.
impl<'a> ser::SerializeTuple for &'a mut Serializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        if !self.output_old_remove_me.ends_with('[') {
            self.output_old_remove_me += ",";
        }
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), DbError> {
        self.output_old_remove_me += "]";
        Ok(())
    }
}

// Same thing but for tuple structs.
impl<'a> ser::SerializeTupleStruct for &'a mut Serializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        if !self.output_old_remove_me.ends_with('[') {
            self.output_old_remove_me += ",";
        }
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), DbError> {
        self.output_old_remove_me += "]";
        Ok(())
    }
}

// Tuple variants are a little different. Refer back to the
// `serialize_tuple_variant` method above:
//
//    self.output_old_remove_me += "{";
//    variant.serialize(&mut *self)?;
//    self.output_old_remove_me += ":[";
//
// So the `end` method in this impl is responsible for closing both the `]` and
// the `}`.
impl<'a> ser::SerializeTupleVariant for &'a mut Serializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        if !self.output_old_remove_me.ends_with('[') {
            self.output_old_remove_me += ",";
        }
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), DbError> {
        self.output_old_remove_me += "]}";
        Ok(())
    }
}

// Some `Serialize` types are not able to hold a key and value in memory at the
// same time so `SerializeMap` implementations are required to support
// `serialize_key` and `serialize_value` individually.
//
// There is a third optional method on the `SerializeMap` trait. The
// `serialize_entry` method allows serializers to optimize for the case where
// key and value are both available simultaneously. In JSON it doesn't make a
// difference so the default behavior for `serialize_entry` is fine.
impl<'a> ser::SerializeMap for &'a mut Serializer {
    type Ok = ();
    type Error = DbError;

    // The Serde data model allows map keys to be any serializable type. JSON
    // only allows string keys so the implementation below will produce invalid
    // JSON if the key serializes as something other than a string.
    //
    // A real JSON serializer would need to validate that map keys are strings.
    // This can be done by using a different Serializer to serialize the key
    // (instead of `&mut **self`) and having that other serializer only
    // implement `serialize_str` and return an error on any other data type.
    fn serialize_key<T>(&mut self, key: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        if !self.output_old_remove_me.ends_with('{') {
            self.output_old_remove_me += ",";
        }
        key.serialize(&mut **self)
    }

    // It doesn't make a difference whether the colon is printed at the end of
    // `serialize_key` or at the beginning of `serialize_value`. In this case
    // the code is a bit simpler having it here.
    fn serialize_value<T>(&mut self, value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        self.output_old_remove_me += ":";
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), DbError> {
        self.output_old_remove_me += "}";
        Ok(())
    }
}

// Structs are like maps in which the keys are constrained to be compile-time
// constant strings.
impl<'a> ser::SerializeStruct for &'a mut Serializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        if !self.output_old_remove_me.ends_with('{') {
            self.output_old_remove_me += ",";
        }
        key.serialize(&mut **self)?;
        self.output_old_remove_me += ":";
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), DbError> {
        self.output_old_remove_me += "}";
        Ok(())
    }
}

// Similar to `SerializeTupleVariant`, here the `end` method is responsible for
// closing both of the curly braces opened by `serialize_struct_variant`.
impl<'a> ser::SerializeStructVariant for &'a mut Serializer {
    type Ok = ();
    type Error = DbError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), DbError>
    where
        T: ?Sized + Serialize,
    {
        if !self.output_old_remove_me.ends_with('{') {
            self.output_old_remove_me += ",";
        }
        key.serialize(&mut **self)?;
        self.output_old_remove_me += ":";
        value.serialize(&mut **self)
    }

    fn end(self) -> Result<(), DbError> {
        self.output_old_remove_me += "}}";
        Ok(())
    }
}

// Deserialization

#[derive(Debug)]
pub struct Deserializer<'de> {
    // This string starts with the input data and characters are truncated off
    // the beginning as data is parsed.
    input: &'de str,
    _input_new: DbRow,
}

impl<'de> Deserializer<'de> {
    // By convention, `Deserializer` constructors are named like `from_xyz`.
    // That way basic use cases are satisfied by something like
    // `serde_json::from_str(...)` while advanced use cases that require a
    // deserializer can make one with `serde_json::Deserializer::from_str(...)`.
    // // pub fn from_str(input: &'de str) -> Self {
    // //     Deserializer {
    // //         input,
    // //         _input_new: DbRow::new(),
    // //     }
    // // }
}

// By convention, the public API of a Serde deserializer is one or more
// `from_xyz` methods such as `from_str`, `from_bytes`, or `from_reader`
// depending on what Rust types the deserializer is able to consume as input.

// // This basic deserializer supports only `from_str`.
// pub fn from_str<'a, T>(s: &'a str) -> Result<T, DbError>
// where
//     T: Deserialize<'a>,
// {
//     let mut deserializer = Deserializer::from_str(s);
//     let t = T::deserialize(&mut deserializer)?;
//     if deserializer.input.is_empty() {
//         Ok(t)
//     } else {
//         Err(DbError::SerdeError(
//             "Deserialization error: TrailingCharacters".to_string(),
//         ))
//     }
// }

pub fn from_db_row<T>(db_row: &DbRow) -> Result<T, DbError>
where
    T: for<'a> Deserialize<'a>,
{
    // TODO: This is a very inefficient way of deserializing a DbRow to a struct, but it works.
    // We need to eliminate the dependency on serde_json and deserialize the DbRow directly to the
    // struct rather than first to a String.

    let string_row = to_string(db_row).unwrap();
    let value: JsonValue = serde_json::from_str(&string_row).unwrap();
    let json_row: JsonRow = value.as_object().unwrap().clone();

    let mut flat_json_row = JsonRow::new();
    for (column, value) in json_row.iter() {
        //println!("VALUE: {value:?}");
        match value.as_object() {
            Some(value) => {
                assert_eq!(value.len(), 1);
                let value = value.iter().collect::<Vec<_>>()[0];
                let value_type = value.0;
                let value_value = JsonValue::from(value.1.clone());
                match value_type.to_lowercase().as_str() {
                    "null" => flat_json_row.insert(column.to_string(), JsonValue::Null),
                    "boolean" => {
                        flat_json_row.insert(column.to_string(), JsonValue::from(value_value))
                    }
                    "smallinteger" => {
                        flat_json_row.insert(column.to_string(), JsonValue::from(value_value))
                    }
                    "integer" => {
                        flat_json_row.insert(column.to_string(), JsonValue::from(value_value))
                    }
                    "biginteger" => {
                        flat_json_row.insert(column.to_string(), JsonValue::from(value_value))
                    }
                    "real" => {
                        flat_json_row.insert(column.to_string(), JsonValue::from(value_value))
                    }
                    "bigreal" => {
                        flat_json_row.insert(column.to_string(), JsonValue::from(value_value))
                    }
                    "numeric" => {
                        flat_json_row.insert(column.to_string(), JsonValue::from(value_value))
                    }
                    "text" => {
                        flat_json_row.insert(column.to_string(), JsonValue::from(value_value))
                    }
                    _ => panic!(),
                }
            }
            None => flat_json_row.insert(column.to_string(), JsonValue::from(value.clone())),
        };
    }
    let flat_string_row = format!("{}", serde_json::json!(flat_json_row));
    let t_struct: T = serde_json::from_str(&flat_string_row).unwrap();
    Ok(t_struct)
}

// SERDE IS NOT A PARSING LIBRARY. This impl block defines a few basic parsing
// functions from scratch. More complicated formats may wish to use a dedicated
// parsing library to help implement their Serde deserializer.
impl<'de> Deserializer<'de> {
    // Look at the first character in the input without consuming it.
    fn peek_char(&mut self) -> Result<char, DbError> {
        self.input.chars().next().ok_or(DbError::SerdeError(
            "Deserialization error: EoF".to_string(),
        ))
    }

    // Consume the first character in the input.
    fn next_char(&mut self) -> Result<char, DbError> {
        let ch = self.peek_char()?;
        self.input = &self.input[ch.len_utf8()..];
        Ok(ch)
    }

    // Parse the JSON identifier `true` or `false`.
    fn parse_bool(&mut self) -> Result<bool, DbError> {
        if self.input.starts_with("true") {
            self.input = &self.input["true".len()..];
            Ok(true)
        } else if self.input.starts_with("false") {
            self.input = &self.input["false".len()..];
            Ok(false)
        } else {
            Err(DbError::SerdeError(
                "Deserialization error: ExpectedBoolean".to_string(),
            ))
        }
    }

    // Parse a group of decimal digits as an unsigned integer of type T.
    //
    // This implementation is a bit too lenient, for example `001` is not
    // allowed in JSON. Also the various arithmetic operations can overflow and
    // panic or return bogus data. But it is good enough for example code!
    fn parse_unsigned<T>(&mut self) -> Result<T, DbError>
    where
        T: AddAssign<T> + MulAssign<T> + From<u8>,
    {
        let mut int = match self.next_char()? {
            ch @ '0'..='9' => T::from(ch as u8 - b'0'),
            _ => {
                return Err(DbError::SerdeError(
                    "Deserialization error: ExpectedInteger".to_string(),
                ));
            }
        };
        loop {
            match self.input.chars().next() {
                Some(ch @ '0'..='9') => {
                    self.input = &self.input[1..];
                    int *= T::from(10);
                    int += T::from(ch as u8 - b'0');
                }
                _ => {
                    return Ok(int);
                }
            }
        }
    }

    // Parse a possible minus sign followed by a group of decimal digits as a
    // signed integer of type T.
    fn parse_signed<T>(&mut self) -> Result<T, DbError>
    where
        T: Neg<Output = T> + AddAssign<T> + MulAssign<T> + From<i8>,
    {
        // Optional minus sign, delegate to `parse_unsigned`, negate if negative.
        unimplemented!()
    }

    // Parse a string until the next '"' character.
    //
    // Makes no attempt to handle escape sequences. What did you expect? This is
    // example code!
    fn parse_string(&mut self) -> Result<&'de str, DbError> {
        if self.next_char()? != '"' {
            return Err(DbError::SerdeError(
                "Deserialization error: ExpectedString".to_string(),
            ));
        }
        match self.input.find('"') {
            Some(len) => {
                let s = &self.input[..len];
                self.input = &self.input[len + 1..];
                Ok(s)
            }
            None => Err(DbError::SerdeError(
                "Deserialization error: EoF".to_string(),
            )),
        }
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a mut Deserializer<'de> {
    type Error = DbError;

    // Look at the input data to decide what Serde data model type to
    // deserialize as. Not all data formats are able to support this operation.
    // Formats that support `deserialize_any` are known as self-describing.
    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        match self.peek_char()? {
            'n' => self.deserialize_unit(visitor),
            't' | 'f' => self.deserialize_bool(visitor),
            '"' => self.deserialize_str(visitor),
            '0'..='9' => self.deserialize_u64(visitor),
            '-' => self.deserialize_i64(visitor),
            '[' => self.deserialize_seq(visitor),
            '{' => {
                println!("IN DESERIALIZE_ANY");
                self.deserialize_map(visitor)
            }
            _ => Err(DbError::SerdeError(
                "Deserialization error: ErrorSyntax".to_string(),
            )),
        }
    }

    // Uses the `parse_bool` parsing function defined above to read the JSON
    // identifier `true` or `false` from the input.
    //
    // Parsing refers to looking at the input and deciding that it contains the
    // JSON value `true` or `false`.
    //
    // Deserialization refers to mapping that JSON value into Serde's data
    // model by invoking one of the `Visitor` methods. In the case of JSON and
    // bool that mapping is straightforward so the distinction may seem silly,
    // but in other cases Deserializers sometimes perform non-obvious mappings.
    // For example the TOML format has a Datetime type and Serde's data model
    // does not. In the `toml` crate, a Datetime in the input is deserialized by
    // mapping it to a Serde data model "struct" type with a special name and a
    // single field containing the Datetime represented as a string.
    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_bool(self.parse_bool()?)
    }

    // The `parse_signed` function is generic over the integer type `T` so here
    // it is invoked with `T=i8`. The next 8 methods are similar.
    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i8(self.parse_signed()?)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i16(self.parse_signed()?)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i32(self.parse_signed()?)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_i64(self.parse_signed()?)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u8(self.parse_unsigned()?)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u16(self.parse_unsigned()?)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u32(self.parse_unsigned()?)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_u64(self.parse_unsigned()?)
    }

    // Float parsing is stupidly hard.
    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // Float parsing is stupidly hard.
    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // The `Serializer` implementation on the previous page serialized chars as
    // single-character strings so handle that representation here.
    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // Parse a string, check that it is one character, call `visit_char`.
        unimplemented!()
    }

    // Refer to the "Understanding deserializer lifetimes" page for information
    // about the three deserialization flavors of strings in Serde.
    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_borrowed_str(self.parse_string()?)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    // The `Serializer` implementation on the previous page serialized byte
    // arrays as JSON arrays of bytes. Handle that representation here.
    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        unimplemented!()
    }

    // An absent optional is represented as the JSON `null` and a present
    // optional is represented as just the contained value.
    //
    // As commented in `Serializer` implementation, this is a lossy
    // representation. For example the values `Some(())` and `None` both
    // serialize as just `null`. Unfortunately this is typically what people
    // expect when working with JSON. Other formats are encouraged to behave
    // more intelligently if possible.
    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        if self.input.starts_with("null") {
            self.input = &self.input["null".len()..];
            visitor.visit_none()
        } else {
            visitor.visit_some(self)
        }
    }

    // In Serde, unit means an anonymous value containing no data.
    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        if self.input.starts_with("null") {
            self.input = &self.input["null".len()..];
            visitor.visit_unit()
        } else {
            Err(DbError::SerdeError(
                "Deserialization error: ExpectedNull".to_string(),
            ))
        }
    }

    // Unit struct means a named value containing no data.
    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        self.deserialize_unit(visitor)
    }

    // As is done here, serializers are encouraged to treat newtype structs as
    // insignificant wrappers around the data they contain. That means not
    // parsing anything other than the contained value.
    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    // Deserialization of compound types like sequences and maps happens by
    // passing the visitor an "Access" object that gives it the ability to
    // iterate through the data contained in the sequence.
    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // Parse the opening bracket of the sequence.
        if self.next_char()? == '[' {
            // Give the visitor access to each element of the sequence.
            let value = visitor.visit_seq(CommaSeparated::new(self))?;
            // Parse the closing bracket of the sequence.
            if self.next_char()? == ']' {
                Ok(value)
            } else {
                Err(DbError::SerdeError(
                    "Deserialization error: ExpectedArrayEnd".to_string(),
                ))
            }
        } else {
            Err(DbError::SerdeError(
                "Deserialization error: ExpectedArray".to_string(),
            ))
        }
    }

    // Tuples look just like sequences in JSON. Some formats may be able to
    // represent tuples more efficiently.
    //
    // As indicated by the length parameter, the `Deserialize` implementation
    // for a tuple in the Serde data model is required to know the length of the
    // tuple before even looking at the input data.
    fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    // Tuple structs look just like sequences in JSON.
    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        self.deserialize_seq(visitor)
    }

    // Much like `deserialize_seq` but calls the visitors `visit_map` method
    // with a `MapAccess` implementation, rather than the visitor's `visit_seq`
    // method with a `SeqAccess` implementation.
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        // Parse the opening brace of the map.
        if self.next_char()? == '{' {
            // Give the visitor access to each entry of the map.
            let value = visitor.visit_map(CommaSeparated::new(self))?;
            // Parse the closing brace of the map.
            if self.next_char()? == '}' {
                Ok(value)
            } else {
                Err(DbError::SerdeError(
                    "Deserialization error: ExpectedMapEnd".to_string(),
                ))
            }
        } else {
            Err(DbError::SerdeError(
                "Deserialization error: ExpectedMap".to_string(),
            ))
        }
    }

    // Structs look just like maps in JSON.
    //
    // Notice the `fields` parameter - a "struct" in the Serde data model means
    // that the `Deserialize` implementation is required to know what the fields
    // are before even looking at the input data. Any key-value pairing in which
    // the fields cannot be known ahead of time is probably a map.
    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        println!("IN DESERIALIZE_STRUCT");
        self.deserialize_map(visitor)
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        if self.peek_char()? == '"' {
            // Visit a unit variant.
            visitor.visit_enum(self.parse_string()?.into_deserializer())
        } else if self.next_char()? == '{' {
            // Visit a newtype variant, tuple variant, or struct variant.
            let value = visitor.visit_enum(Enum::new(self))?;
            // Parse the matching close brace.
            if self.next_char()? == '}' {
                Ok(value)
            } else {
                Err(DbError::SerdeError(
                    "Deserialization error: ExpectedMapEnd".to_string(),
                ))
            }
        } else {
            Err(DbError::SerdeError(
                "Deserialization error: ExpectedMap".to_string(),
            ))
        }
    }

    // An identifier in Serde is the type that identifies a field of a struct or
    // the variant of an enum. In JSON, struct fields and enum variants are
    // represented as strings. In other formats they may be represented as
    // numeric indices.
    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        self.deserialize_str(visitor)
    }

    // Like `deserialize_any` but indicates to the `Deserializer` that it makes
    // no difference which `Visitor` method is called because the data is
    // ignored.
    //
    // Some deserializers are able to implement this more efficiently than
    // `deserialize_any`, for example by rapidly skipping over matched
    // delimiters without paying close attention to the data in between.
    //
    // Some formats are not able to implement this at all. Formats that can
    // implement `deserialize_any` and `deserialize_ignored_any` are known as
    // self-describing.
    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        self.deserialize_any(visitor)
    }
}

// In order to handle commas correctly when deserializing a JSON array or map,
// we need to track whether we are on the first element or past the first
// element.
struct CommaSeparated<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
    first: bool,
}

impl<'a, 'de> CommaSeparated<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>) -> Self {
        CommaSeparated { de, first: true }
    }
}

// `SeqAccess` is provided to the `Visitor` to give it the ability to iterate
// through elements of the sequence.
impl<'de, 'a> SeqAccess<'de> for CommaSeparated<'a, 'de> {
    type Error = DbError;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, DbError>
    where
        T: DeserializeSeed<'de>,
    {
        // Check if there are no more elements.
        if self.de.peek_char()? == ']' {
            return Ok(None);
        }
        // Comma is required before every element except the first.
        if !self.first && self.de.next_char()? != ',' {
            return Err(DbError::SerdeError(
                "Deserialization error: ExpectedArrayComma".to_string(),
            ));
        }
        self.first = false;
        // Deserialize an array element.
        seed.deserialize(&mut *self.de).map(Some)
    }
}

// `MapAccess` is provided to the `Visitor` to give it the ability to iterate
// through entries of the map.
impl<'de, 'a> MapAccess<'de> for CommaSeparated<'a, 'de> {
    type Error = DbError;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, DbError>
    where
        K: DeserializeSeed<'de>,
    {
        // Check if there are no more entries.
        if self.de.peek_char()? == '}' {
            return Ok(None);
        }
        // Comma is required before every entry except the first.
        if !self.first && self.de.next_char()? != ',' {
            return Err(DbError::SerdeError(
                "Deserialization error: ExpectedMapComma".to_string(),
            ));
        }
        self.first = false;
        // Deserialize a map key.
        seed.deserialize(&mut *self.de).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, DbError>
    where
        V: DeserializeSeed<'de>,
    {
        // It doesn't make a difference whether the colon is parsed at the end
        // of `next_key_seed` or at the beginning of `next_value_seed`. In this
        // case the code is a bit simpler having it here.
        if self.de.next_char()? != ':' {
            return Err(DbError::SerdeError(
                "Deserialization error: ExpectedMapColon".to_string(),
            ));
        }
        // Deserialize a map value.
        seed.deserialize(&mut *self.de)
    }
}

struct Enum<'a, 'de: 'a> {
    de: &'a mut Deserializer<'de>,
}

impl<'a, 'de> Enum<'a, 'de> {
    fn new(de: &'a mut Deserializer<'de>) -> Self {
        Enum { de }
    }
}

// `EnumAccess` is provided to the `Visitor` to give it the ability to determine
// which variant of the enum is supposed to be deserialized.
//
// Note that all enum deserialization methods in Serde refer exclusively to the
// "externally tagged" enum representation.
impl<'de, 'a> EnumAccess<'de> for Enum<'a, 'de> {
    type Variant = Self;
    type Error = DbError;

    fn variant_seed<V>(self, seed: V) -> Result<(V::Value, Self::Variant), DbError>
    where
        V: DeserializeSeed<'de>,
    {
        // The `deserialize_enum` method parsed a `{` character so we are
        // currently inside of a map. The seed will be deserializing itself from
        // the key of the map.
        let val = seed.deserialize(&mut *self.de)?;
        // Parse the colon separating map key from value.
        if self.de.next_char()? == ':' {
            Ok((val, self))
        } else {
            Err(DbError::SerdeError(
                "Deserialization error: ExpectedMapColon".to_string(),
            ))
        }
    }
}

// `VariantAccess` is provided to the `Visitor` to give it the ability to see
// the content of the single variant that it decided to deserialize.
impl<'de, 'a> VariantAccess<'de> for Enum<'a, 'de> {
    type Error = DbError;

    // If the `Visitor` expected this variant to be a unit variant, the input
    // should have been the plain string case handled in `deserialize_enum`.
    fn unit_variant(self) -> Result<(), DbError> {
        Err(DbError::SerdeError(
            "Deserialization error: ExpectedString".to_string(),
        ))
    }

    // Newtype variants are represented in JSON as `{ NAME: VALUE }` so
    // deserialize the value here.
    fn newtype_variant_seed<T>(self, seed: T) -> Result<T::Value, DbError>
    where
        T: DeserializeSeed<'de>,
    {
        seed.deserialize(self.de)
    }

    // Tuple variants are represented in JSON as `{ NAME: [DATA...] }` so
    // deserialize the sequence of data here.
    fn tuple_variant<V>(self, _len: usize, visitor: V) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        de::Deserializer::deserialize_seq(self.de, visitor)
    }

    // Struct variants are represented in JSON as `{ NAME: { K: V, ... } }` so
    // deserialize the inner map here.
    fn struct_variant<V>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, DbError>
    where
        V: Visitor<'de>,
    {
        println!("IN STRUCT_VARIANT");
        de::Deserializer::deserialize_map(self.de, visitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db_row, db_value::DbValue};

    #[test]
    fn test_serde_struct() {
        let db_row = db_row! {"value".into() => DbValue::from("foo")};
        let expected_string = r#"{"value":{"Text":"foo"}}"#;
        assert_eq!(to_string(&db_row).unwrap(), expected_string);
        assert_eq!(to_db_row(&db_row).unwrap(), db_row);

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct Foo {
            bar: u32,
            xyzzy: String,
        }

        let expected_struct = Foo {
            bar: 1,
            xyzzy: "test".to_string(),
        };
        let expected_string = r#"{"bar":1,"xyzzy":"test"}"#;
        assert_eq!(to_string(&expected_struct).unwrap(), expected_string);

        let expected_db_row = db_row! {
            "bar".into() => DbValue::from(1_u32),
            "xyzzy".into() => DbValue::from("test"),
        };
        assert_eq!(to_db_row(&expected_struct).unwrap(), expected_db_row);

        let db_row = db_row! {
            "bar".into() => DbValue::from(1_u32),
            "xyzzy".into() => DbValue::from("test"),
        };
        assert_eq!(expected_struct, from_db_row(&db_row).unwrap());
    }
}
