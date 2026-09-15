//! Implement `serde` for values and rows.

use crate::{Error, JsonValue, Value, ValueType};
use serde::{Deserialize, Serialize};
use serde_json::value::Serializer as JsonValueSerializer;

/// TODO: Add docstring.
pub fn to_value<T>(value: &T) -> Result<Value, Error>
where
    T: Serialize,
{
    let json_serializer = JsonValueSerializer;
    let json_value = value
        .serialize(json_serializer)
        .map_err(|err| Error::SerdeError(err.to_string()))?;
    match json_value {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(val) => Ok(Value::Boolean(val)),
        JsonValue::Number(val) => match val.as_u64() {
            Some(val) => {
                let min_type = ValueType::min_integer().min_type(&val.to_string()).unwrap();
                match min_type {
                    ValueType::SmallInteger(_) => Ok(Value::SmallInteger(val as i16)),
                    ValueType::Integer(_) => Ok(Value::Integer(val as i32)),
                    ValueType::BigInteger(_) => Ok(Value::BigInteger(val as i64)),
                    _ => panic!(),
                }
            }
            None => match val.as_f64() {
                Some(val) => {
                    let min_type = ValueType::min_real().min_type(&val.to_string()).unwrap();
                    match min_type {
                        ValueType::Real(_) => Ok(Value::Real(val as f32)),
                        ValueType::BigReal(_) => Ok(Value::BigReal(val as f64)),
                        _ => panic!(),
                    }
                }
                None => panic!(),
            },
        },
        JsonValue::String(val) => Ok(Value::Text(val)),
        JsonValue::Array(_) => Ok(Value::Json(json_value)),
        JsonValue::Object(_) => Ok(Value::Json(json_value)),
    }
}

pub fn from_value<T>(value: &Value) -> Result<T, Error>
where
    T: for<'de> Deserialize<'de>,
{
    match value {
        Value::Json(json_value) => Ok(serde_json::from_str(&json_value.to_string()).unwrap()),
        Value::Null => Ok(serde_json::from_str("null").unwrap()),
        Value::Text(text) => Ok(serde_json::from_str(&text.as_str().to_string()).unwrap()),
        _ => {
            let value = value.to_string();
            Ok(serde_json::from_str(&value).unwrap())
        }
    }
}

// TODO: Move tests to unit_tests.rs later.
#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::{Decimal, dec};
    use serde_json::json;
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
        let value: HashMap<String, bool> = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);

        let expected_deserialized = HashMap::<String, u64>::new();
        let expected_serialized = Value::Json(json!({}));
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        let value: HashMap<String, u64> = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);

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

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct UnitStruct;

        #[derive(Deserialize, Serialize, PartialEq, Debug, Clone)]
        struct NormalStruct {
            foo: String,
            bar: u64,
            list: Vec<i16>,
            tuple: (u64, String),
            json: JsonValue,
        }

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

        // Serializing and deserializing an arbitrary struct to a Row:
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

            // i8
            tinyint: i8,
            tinyint_opt_none: Option<i8>,
            tinyint_opt_some: Option<i8>,

            // i16
            smallint: i16,
            smallint_opt_none: Option<i16>,
            smallint_opt_some: Option<i16>,

            // i32
            mediumint: i32,
            mediumint_opt_none: Option<i32>,
            mediumint_opt_some: Option<i32>,

            // i64
            bigint: i64,
            bigint_opt_none: Option<i64>,
            bigint_opt_some: Option<i64>,

            // u8
            tiny_unsigned: u8,
            tiny_unsigned_opt_none: Option<u8>,
            tiny_unsigned_opt_some: Option<u8>,

            // u16
            small_unsigned: u16,
            small_unsigned_opt_none: Option<u16>,
            small_unsigned_opt_some: Option<u16>,

            // u32
            medium_unsigned: u32,
            medium_unsigned_opt_none: Option<u32>,
            medium_unsigned_opt_some: Option<u32>,

            // u64
            big_unsigned: u64,
            big_unsigned_opt_none: Option<u64>,
            big_unsigned_opt_some: Option<u64>,

            // f32
            smallfloat: f32,
            smallfloat_opt_none: Option<f32>,
            smallfloat_opt_some: Option<f32>,

            // f64
            bigfloat: f64,
            bigfloat_opt_none: Option<f64>,
            bigfloat_opt_some: Option<f64>,

            // String
            text: String,
            text_opt_none: Option<String>,
            text_opt_some: Option<String>,

            // Note that rust's serializer serializes Decimal values to text (see also below).
            biggerfloat: Decimal,
            biggerfloat_opt_none: Option<Decimal>,
            biggerfloat_opt_some: Option<Decimal>,

            // A vector of u8 (bytes)
            sequence: Vec<u8>,
            sequence_opt_none: Option<Vec<u8>>,
            sequence_opt_some: Option<Vec<u8>>,

            // Enum
            enumeration: Enumeration,
            enumeration_opt_none: Option<Enumeration>,
            enumeration_opt_some: Option<Enumeration>,

            // Struct variant
            struct_variant: Enumeration,
            struct_variant_opt_none: Option<Enumeration>,
            struct_variant_opt_some: Option<Enumeration>,

            // Tuple variant
            tuple_variant: Enumeration,
            tuple_variant_opt_none: Option<Enumeration>,
            tuple_variant_opt_some: Option<Enumeration>,

            // // Unit struct
            // unit_struct: UnitStruct,
            // unit_struct_opt_none: Option<UnitStruct>,
            // unit_struct_opt_some: Option<UnitStruct>,

            // Struct
            structure: NormalStruct,
            structure_opt_none: Option<NormalStruct>,
            structure_opt_some: Option<NormalStruct>,

            // Newtype struct
            newtype_struct: NewTypeStruct,
            newtype_struct_opt_none: Option<NewTypeStruct>,
            newtype_struct_opt_some: Option<NewTypeStruct>,

            // Tuple
            tuple: (u32, String),
            tuple_opt_none: Option<(u32, String)>,
            tuple_opt_some: Option<(u32, String)>,

            // Tuple struct
            tuple_struct: TupleStruct,
            tuple_struct_opt_none: Option<TupleStruct>,
            tuple_struct_opt_some: Option<TupleStruct>,

            // Nested struct
            nested_struct: NestedStruct,
            nested_struct_opt_none: Option<NestedStruct>,
            nested_struct_opt_some: Option<NestedStruct>,
        }

        let expected_deserialized = TestStruct {
            json_simple_1: json!(1724),
            json_simple_2: JsonValue::Null,
            json_simple_3: JsonValue::Bool(true),
            json_simple_4: JsonValue::String("foo".into()),
            json_complex: json!({"alpha": 10}),
            json_opt_none: None,
            json_opt_some: Some(json!({})),

            boolean: true,
            boolean_opt_none: None,
            boolean_opt_some: Some(true),

            tinyint: 1,
            tinyint_opt_none: None,
            tinyint_opt_some: Some(1),

            smallint: 1,
            smallint_opt_none: None,
            smallint_opt_some: Some(1),

            mediumint: 1,
            mediumint_opt_none: None,
            mediumint_opt_some: Some(1),

            bigint: 1,
            bigint_opt_none: None,
            bigint_opt_some: Some(1),

            tiny_unsigned: 1,
            tiny_unsigned_opt_none: None,
            tiny_unsigned_opt_some: Some(1),

            small_unsigned: 1,
            small_unsigned_opt_none: None,
            small_unsigned_opt_some: Some(1),

            medium_unsigned: 1,
            medium_unsigned_opt_none: None,
            medium_unsigned_opt_some: Some(1),

            big_unsigned: 1,
            big_unsigned_opt_none: None,
            big_unsigned_opt_some: Some(1),

            smallfloat: 1_f32,
            smallfloat_opt_none: None,
            smallfloat_opt_some: Some(1_f32),

            bigfloat: 1_f64,
            bigfloat_opt_none: None,
            bigfloat_opt_some: Some(1_f64),

            text: 1.to_string(),
            text_opt_none: None,
            text_opt_some: Some(1.to_string()),

            biggerfloat: dec!(1),
            biggerfloat_opt_none: None,
            biggerfloat_opt_some: Some(dec!(1)),

            // Compound types:
            sequence: vec![0, 1, 2],
            sequence_opt_none: None,
            sequence_opt_some: Some(vec![0, 1, 2]),

            enumeration: Enumeration::NewTypeVariant(1.0),
            enumeration_opt_none: None,
            enumeration_opt_some: Some(Enumeration::UnitVariant),

            struct_variant: Enumeration::StructVariant(BasicStruct { foo: 1 }),
            struct_variant_opt_none: None,
            struct_variant_opt_some: Some(Enumeration::StructVariant(BasicStruct { foo: 1 })),

            tuple_variant: Enumeration::TupleVariant(1, 1),
            tuple_variant_opt_none: None,
            tuple_variant_opt_some: Some(Enumeration::TupleVariant(1, 1)),

            //unit_struct: UnitStruct,
            //unit_struct_opt_none: None,
            //unit_struct_opt_some: Some(UnitStruct),
            structure: NormalStruct {
                foo: String::from("bar"),
                bar: 1,
                list: vec![1, 2, 3],
                tuple: (1, String::from("bar")),
                json: json!([13, 14.0]),
            },
            structure_opt_none: None,
            structure_opt_some: Some(NormalStruct {
                foo: String::from("bar"),
                bar: 1,
                list: vec![1, 2, 3],
                tuple: (1, String::from("bar")),
                json: json!({"foo": 13.2, "bar": "yes"}),
            }),

            newtype_struct: NewTypeStruct(1),
            newtype_struct_opt_none: None,
            newtype_struct_opt_some: Some(NewTypeStruct(1)),

            tuple: (1, String::from("bar")),
            tuple_opt_none: None,
            tuple_opt_some: Some((1, String::from("bar"))),

            tuple_struct: TupleStruct(111, 111),
            tuple_struct_opt_none: None,
            tuple_struct_opt_some: Some(TupleStruct(111, 111)),

            nested_struct: NestedStruct {
                foo: Enumeration::NewTypeVariant(1.0),
                bar: NormalStruct {
                    foo: String::from("bar"),
                    bar: 1,
                    list: vec![1, 2, 3],
                    tuple: (1, String::from("bar")),
                    json: json!([]),
                },
                foo_list: vec![Enumeration::NewTypeVariant(1.0)],
                bar_list: vec![NormalStruct {
                    foo: String::from("bar"),
                    bar: 1,
                    list: vec![1, 2, 3],
                    tuple: (1, String::from("bar")),
                    json: json!([JsonValue::Null, json!({})]),
                }],
                bar_tuple: (1, 1, String::from("bar")),
            },
            nested_struct_opt_none: None,
            nested_struct_opt_some: Some(NestedStruct {
                foo: Enumeration::NewTypeVariant(1.0),
                bar: NormalStruct {
                    foo: String::from("bar"),
                    bar: 1,
                    list: vec![1, 2, 3],
                    tuple: (1, String::from("bar")),
                    json: JsonValue::Null,
                },
                foo_list: vec![Enumeration::NewTypeVariant(1.0)],
                bar_list: vec![NormalStruct {
                    foo: String::from("bar"),
                    bar: 1,
                    list: vec![1, 2, 3],
                    tuple: (1, String::from("bar")),
                    json: json!("Semmelweis"),
                }],
                bar_tuple: (1, 1, String::from("bar")),
            }),
        };
        let expected_serialized = Value::Json(json!({
            "json_simple_1": 1724,
            "json_simple_2": JsonValue::Null,
            "json_simple_3": true,
            "json_simple_4": "foo",
            "json_complex": {"alpha": 10},
            "json_opt_none": JsonValue::Null,
            "json_opt_some": {},

            "boolean": true,
            "boolean_opt_none": JsonValue::Null,
            "boolean_opt_some": true,

            "tinyint": 1,
            "tinyint_opt_none": JsonValue::Null,
            "tinyint_opt_some": 1,

            "smallint": 1,
            "smallint_opt_none": JsonValue::Null,
            "smallint_opt_some": 1,

            "mediumint": 1,
            "mediumint_opt_none": JsonValue::Null,
            "mediumint_opt_some": 1,

            "bigint": 1,
            "bigint_opt_none": JsonValue::Null,
            "bigint_opt_some": 1,

            "tiny_unsigned": 1,
            "tiny_unsigned_opt_none": JsonValue::Null,
            "tiny_unsigned_opt_some": 1,

            "small_unsigned": 1,
            "small_unsigned_opt_none": JsonValue::Null,
            "small_unsigned_opt_some": 1,

            "medium_unsigned": 1,
            "medium_unsigned_opt_none": JsonValue::Null,
            "medium_unsigned_opt_some": 1,

            "big_unsigned": 1,
            "big_unsigned_opt_none": JsonValue::Null,
            "big_unsigned_opt_some": 1,

            "smallfloat": 1_f32,
            "smallfloat_opt_none": JsonValue::Null,
            "smallfloat_opt_some": 1_f32,

            "bigfloat": 1_f64,
            "bigfloat_opt_none": JsonValue::Null,
            "bigfloat_opt_some": 1_f64,

            "text": 1.to_string(),
            "text_opt_none": JsonValue::Null,
            "text_opt_some": 1.to_string(),

            "biggerfloat": "1",
            "biggerfloat_opt_none": JsonValue::Null,
            "biggerfloat_opt_some": "1",

            "sequence": [0, 1, 2],
            "sequence_opt_none": JsonValue::Null,
            "sequence_opt_some": [0, 1, 2],

            "enumeration": {"NewTypeVariant": 1.0},
            "enumeration_opt_none": JsonValue::Null,
            "enumeration_opt_some": "UnitVariant",

            "struct_variant": {"StructVariant":{"foo": 1}},
            "struct_variant_opt_none": JsonValue::Null,
            "struct_variant_opt_some": {"StructVariant":{"foo": 1}},

            "tuple_variant": {"TupleVariant":[1,1]},
            "tuple_variant_opt_none": JsonValue::Null,
            "tuple_variant_opt_some": {"TupleVariant":[1, 1]},

            //"unit_struct": "UnitStruct",
            //"unit_struct_opt_none": JsonValue::Null,
            //"unit_struct_opt_some": "UnitStruct",

            "structure": {
                "foo": "bar",
                "bar": 1,
                "list": [1, 2, 3],
                "tuple": [1,"bar"],
                "json": [13, 14.0]
            },
            "structure_opt_none": JsonValue::Null,
            "structure_opt_some": {
                "foo": "bar",
                "bar": 1,
                "list": [1, 2, 3],
                "tuple": [1, "bar"],
                "json": {"foo": 13.2, "bar": "yes"}
            },

            "newtype_struct": 1_i64,
            "newtype_struct_opt_none": JsonValue::Null,
            "newtype_struct_opt_some": 1_i64,

            "tuple": [1,"bar"],
            "tuple_opt_none": JsonValue::Null,
            "tuple_opt_some": [1,"bar"],


            "tuple_struct": [111,111],
            "tuple_struct_opt_none": JsonValue::Null,
            "tuple_struct_opt_some": [111,111],

            "nested_struct": {
                "foo": {"NewTypeVariant":1.0},
                "bar": {
                    "foo": "bar",
                    "bar": 1,
                    "list": [1, 2, 3],
                    "tuple": [1, "bar"],
                    "json": []
                },
                "foo_list":[{"NewTypeVariant": 1.0}],
                "bar_list":[{
                    "foo": "bar",
                    "bar": 1,
                    "list": [1, 2, 3],
                    "tuple": [1, "bar"],
                    "json": [JsonValue::Null, {}]
                }],
                "bar_tuple": [1, 1, "bar"]
            },
            "nested_struct_opt_none": JsonValue::Null,
            "nested_struct_opt_some": {
                "foo": {"NewTypeVariant": 1.0},
                "bar": {
                    "foo": "bar",
                    "bar": 1,
                    "list": [1, 2, 3],
                    "tuple": [1, "bar"],
                    "json": JsonValue::Null
                },
                "foo_list":[{"NewTypeVariant": 1.0}],
                "bar_list":[{
                    "foo": "bar",
                    "bar": 1,
                    "list": [1, 2, 3],
                    "tuple": [1, "bar"],
                    "json": "Semmelweis"
                }],
                "bar_tuple": [1, 1, "bar"]
            }
        }));
        let value = to_value(&expected_deserialized).unwrap();
        assert_eq!(value, expected_serialized);
        let value: TestStruct = from_value(&value).unwrap();
        assert_eq!(value, expected_deserialized);
    }
}
