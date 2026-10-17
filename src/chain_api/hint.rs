//! Routing hints: the block a block-endpoint body names.
//!
//! nodeos reads bodies with fc's legacy JSON parser, which differs from
//! standard JSON in ways that change the block a body names: the escape
//! `\u0031` reads as the text `u0031`, a repeated key keeps its first
//! value, `1e5` is a string, trailing bytes are ignored. A hint is therefore
//! produced only for bodies that fc and a strict parser read the same way:
//! standard JSON with no backslash, no repeated top-level key, no
//! non-integer number, and a block value in a plain form. Every other body
//! gets no hint and is answered by one live node, which writes its own error
//! if the body is bad. The router never guesses a block, and never invents a
//! parameter error.

use std::collections::HashSet;
use std::fmt;

use serde::de::{self, DeserializeSeed, IgnoredAny, MapAccess, SeqAccess, Visitor};

use super::route::BlockEndpoint;

/// nodeos's empty-body test (`is_empty_content`): after trimming spaces
/// (only spaces) the body is empty, or is `{`, then spaces and tabs, then
/// `}`. A body that is empty this way is "A Request body is required".
pub fn is_empty_content(body: &[u8]) -> bool {
    let start = body.iter().position(|&b| b != b' ');
    let Some(start) = start else {
        return true;
    };
    let end = body.iter().rposition(|&b| b != b' ').unwrap_or(start);
    let trimmed = &body[start..=end];
    trimmed.len() > 1
        && trimmed[0] == b'{'
        && trimmed[trimmed.len() - 1] == b'}'
        && trimmed[1..trimmed.len() - 1]
            .iter()
            .all(|&b| b == b' ' || b == b'\t')
}

/// The block a body names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Hint {
    /// The block number: the number given, or an id's first four bytes.
    pub num: u32,
    /// The body names the block by id, which nodeos finds only if it is the
    /// id of its block `num`.
    pub by_id: bool,
}

impl Hint {
    fn num(num: u32) -> Hint {
        Hint { num, by_id: false }
    }

    fn id(num: u32) -> Hint {
        Hint { num, by_id: true }
    }
}

/// The block `body` names for endpoint `ep`, or `None` when the router must
/// not route on it.
pub fn hint(ep: BlockEndpoint, body: &[u8]) -> Option<Hint> {
    if is_empty_content(body) || body.contains(&b'\\') {
        return None;
    }
    let key = match ep {
        BlockEndpoint::BlockInfo => "block_num",
        _ => "block_num_or_id",
    };
    let value = top_level_value(body, key)?;
    let number = decimal(&value).map(Hint::num);
    match ep {
        BlockEndpoint::BlockInfo => number,
        BlockEndpoint::BlockHeaderState => number.or_else(|| match &value {
            Value::Str(s) if s.len() == 64 => hex_block_num(s).map(Hint::id),
            _ => None,
        }),
        BlockEndpoint::Block | BlockEndpoint::BlockHeader | BlockEndpoint::RawBlock => number
            .or_else(|| match &value {
                Value::Str(s) => hex_block_num(s).map(Hint::id),
                _ => None,
            }),
    }
}

/// A JSON integer >= 0, or a string of 1 to 64 ASCII digits whose value
/// fits in u64. fc reads such a string as a number whatever its length
/// (boost `lexical_cast` skips leading zeros), so `"0000000000000000000005"`
/// is block 5, not an id. Truncated to u32, as nodeos looks the block up.
fn decimal(value: &Value) -> Option<u32> {
    match value {
        Value::UInt(v) => Some(*v as u32),
        Value::Str(s) if (1..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit()) => {
            s.parse::<u64>().ok().map(|v| v as u32)
        }
        _ => None,
    }
}

/// An id of 1 to 32 hex byte pairs (either case): nodeos copies it into a
/// zeroed 32-byte id, whose first four bytes are the block number.
fn hex_block_num(s: &str) -> Option<u32> {
    if s.is_empty()
        || s.len() > 64
        || !s.len().is_multiple_of(2)
        || !s.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return None;
    }
    let mut digits = [b'0'; 8];
    let head = &s.as_bytes()[..s.len().min(8)];
    digits[..head.len()].copy_from_slice(head);
    u32::from_str_radix(std::str::from_utf8(&digits).ok()?, 16).ok()
}

/// The hint key's value, as far as the hint rules care.
#[derive(Debug, PartialEq, Eq)]
enum Value {
    UInt(u64),
    Str(String),
    /// Anything else: negative, bool, null, object, array.
    Other,
}

/// The value of top-level `key` in a strict JSON object, or `None` when the
/// body is not one, repeats a top-level key, holds a non-integer number
/// anywhere, nests deeper than fc parses, or has no `key`.
fn top_level_value(body: &[u8], key: &'static str) -> Option<Value> {
    let mut de = serde_json::Deserializer::from_slice(body);
    let value = TopLevel { key }.deserialize(&mut de).ok()?;
    de.end().ok()?;
    value
}

/// The nesting level (the top-level object is level 1) at which a container
/// may no longer hold anything. fc's legacy parser starts with a depth
/// budget of 200 and spends two units per level: `variant_from_stream`
/// passes `max_depth - 1` to the container parser, which passes
/// `max_depth - 1` again to each member or element. So the values inside a
/// level-100 container are parsed with a budget of 0, which fails. An empty
/// container at that level parses.
const FC_FULL_LEVEL: u32 = 100;

/// Reads the top-level object.
struct TopLevel {
    key: &'static str,
}

impl<'de> DeserializeSeed<'de> for TopLevel {
    type Value = Option<Value>;

    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        d.deserialize_map(self)
    }
}

impl<'de> Visitor<'de> for TopLevel {
    type Value = Option<Value>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut seen: HashSet<&'de str> = HashSet::new();
        let mut found = None;
        // Keys borrow from the body: they hold no escapes, as the body holds
        // no backslash.
        while let Some(key) = map.next_key::<&'de str>()? {
            if !seen.insert(key) {
                return Err(de::Error::custom("repeated top-level key"));
            }
            // A member value that is a container opens level 2.
            if key == self.key {
                found = Some(map.next_value_seed(HintValue { level: 2 })?);
            } else {
                map.next_value_seed(Nested { level: 2 })?;
            }
        }
        Ok(found)
    }
}

/// Any JSON value with no non-integer number in it and no deeper nesting
/// than fc parses. `level` is the nesting level the value has if it is a
/// container. Integers that do not fit u64 or i64 (and `-0`) also arrive as
/// floats and are refused.
#[derive(Clone, Copy)]
struct Nested {
    level: u32,
}

impl Nested {
    fn too_deep<E: de::Error>() -> E {
        E::custom("nested deeper than fc parses")
    }
}

impl<'de> DeserializeSeed<'de> for Nested {
    type Value = ();

    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<(), D::Error> {
        d.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Nested {
    type Value = ();

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value without non-integer numbers")
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<(), E> {
        Ok(())
    }
    fn visit_i64<E: de::Error>(self, _: i64) -> Result<(), E> {
        Ok(())
    }
    fn visit_u64<E: de::Error>(self, _: u64) -> Result<(), E> {
        Ok(())
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<(), E> {
        Err(E::custom("non-integer number"))
    }
    fn visit_str<E: de::Error>(self, _: &str) -> Result<(), E> {
        Ok(())
    }
    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Ok(())
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        if self.level >= FC_FULL_LEVEL {
            return match seq.next_element::<IgnoredAny>()? {
                Some(_) => Err(Self::too_deep()),
                None => Ok(()),
            };
        }
        let inner = Nested {
            level: self.level + 1,
        };
        while seq.next_element_seed(inner)?.is_some() {}
        Ok(())
    }
    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        if self.level >= FC_FULL_LEVEL {
            return match map.next_key::<IgnoredAny>()? {
                Some(_) => Err(Self::too_deep()),
                None => Ok(()),
            };
        }
        let inner = Nested {
            level: self.level + 1,
        };
        while map.next_key::<IgnoredAny>()?.is_some() {
            map.next_value_seed(inner)?;
        }
        Ok(())
    }
}

/// The hint key's value; refuses what [`Nested`] refuses.
#[derive(Clone, Copy)]
struct HintValue {
    level: u32,
}

impl<'de> DeserializeSeed<'de> for HintValue {
    type Value = Value;

    fn deserialize<D: de::Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
        d.deserialize_any(HintValueVisitor { level: self.level })
    }
}

struct HintValueVisitor {
    level: u32,
}

impl<'de> Visitor<'de> for HintValueVisitor {
    type Value = Value;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a JSON value without non-integer numbers")
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<Value, E> {
        Ok(Value::Other)
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<Value, E> {
        // serde_json reports non-negative integers through `visit_u64`; this
        // is a negative one.
        Ok(match u64::try_from(v) {
            Ok(v) => Value::UInt(v),
            Err(_) => Value::Other,
        })
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<Value, E> {
        Ok(Value::UInt(v))
    }
    fn visit_f64<E: de::Error>(self, _: f64) -> Result<Value, E> {
        Err(E::custom("non-integer number"))
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<Value, E> {
        Ok(Value::Str(v.to_owned()))
    }
    fn visit_unit<E: de::Error>(self) -> Result<Value, E> {
        Ok(Value::Other)
    }
    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<Value, A::Error> {
        Nested { level: self.level }.visit_seq(seq)?;
        Ok(Value::Other)
    }
    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<Value, A::Error> {
        Nested { level: self.level }.visit_map(map)?;
        Ok(Value::Other)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RAW: BlockEndpoint = BlockEndpoint::RawBlock;
    const INFO: BlockEndpoint = BlockEndpoint::BlockInfo;
    const BHS: BlockEndpoint = BlockEndpoint::BlockHeaderState;

    fn h(ep: BlockEndpoint, body: &str) -> Option<u32> {
        hint(ep, body.as_bytes()).map(|h| h.num)
    }

    /// Whether the body names its block by id.
    fn by_id(ep: BlockEndpoint, body: &str) -> Option<bool> {
        hint(ep, body.as_bytes()).map(|h| h.by_id)
    }

    /// An id of block 480385480 (0x1ca219c8).
    const SAMPLE_ID: &str = "1ca219c8d6fbbd1a3e0a5fe9e7b1b8e1e5a3d6b1c7e2f4a9b0c1d2e3f4a5b6c7";

    // CU2
    #[test]
    fn empty_content_follows_nodeos() {
        for empty in ["", " ", "   ", "{}", "{ }", "{ \t }", " {} ", "  {\t\t}  "] {
            assert!(is_empty_content(empty.as_bytes()), "{empty:?}");
        }
        for not_empty in [
            "{}\n",
            "\t{}",
            "\n",
            "{",
            "}",
            "{ \n }",
            "{\"a\":1}",
            "x",
            " x ",
        ] {
            assert!(!is_empty_content(not_empty.as_bytes()), "{not_empty:?}");
        }
        for ep in BlockEndpoint::ALL {
            assert_eq!(hint(ep, b"{}"), None);
            assert_eq!(hint(ep, b"  "), None);
        }
    }

    // CU2
    #[test]
    fn block_num_or_id_numbers_and_digit_strings() {
        for ep in [
            BlockEndpoint::Block,
            BlockEndpoint::BlockHeader,
            BlockEndpoint::RawBlock,
            BHS,
        ] {
            assert_eq!(h(ep, r#"{"block_num_or_id":1}"#), Some(1), "{ep:?}");
            assert_eq!(h(ep, r#"{"block_num_or_id":"1"}"#), Some(1), "{ep:?}");
            assert_eq!(h(ep, r#"{"block_num_or_id":"0001"}"#), Some(1), "{ep:?}");
            assert_eq!(h(ep, r#"{"block_num_or_id":0}"#), Some(0), "{ep:?}");
            // u32 truncation, as nodeos looks the block up.
            assert_eq!(
                h(ep, r#"{"block_num_or_id":"4294967297"}"#),
                Some(1),
                "{ep:?}"
            );
            assert_eq!(
                h(ep, r#"{"block_num_or_id":4294967297}"#),
                Some(1),
                "{ep:?}"
            );
            assert_eq!(
                h(ep, r#"{"block_num_or_id":"18446744073709551615"}"#),
                Some(u32::MAX),
                "{ep:?}"
            );
            assert_eq!(
                h(ep, r#"{"block_num_or_id":18446744073709551615}"#),
                Some(u32::MAX),
                "{ep:?}"
            );
            // Surrounding whitespace and other keys do not matter.
            assert_eq!(
                h(
                    ep,
                    "\r\n\t {\"x\": [1, {\"y\": null}], \"block_num_or_id\" : \"7\" }\n"
                ),
                Some(7),
                "{ep:?}"
            );
            assert_eq!(
                h(ep, r#"{"block_num_or_id":"7","include_extensions":true}"#),
                Some(7),
                "{ep:?}"
            );
        }
    }

    // CU2
    #[test]
    fn hex_ids_name_the_block_in_their_first_four_bytes() {
        assert_eq!(
            h(RAW, &format!(r#"{{"block_num_or_id":"{SAMPLE_ID}"}}"#)),
            Some(480_385_480)
        );
        let upper = SAMPLE_ID.to_ascii_uppercase();
        assert_eq!(
            h(RAW, &format!(r#"{{"block_num_or_id":"{upper}"}}"#)),
            Some(480_385_480)
        );
        // A short id is copied into a zeroed id.
        assert_eq!(h(RAW, r#"{"block_num_or_id":"0a"}"#), Some(0x0a00_0000));
        assert_eq!(h(RAW, r#"{"block_num_or_id":"0a0b0c"}"#), Some(0x0a0b_0c00));
        assert_eq!(h(RAW, r#"{"block_num_or_id":"ab"}"#), Some(0xab00_0000));
        // Digits above u64::MAX are an id to nodeos too: 20 digits, even.
        assert_eq!(
            h(RAW, r#"{"block_num_or_id":"99999999999999999999"}"#),
            Some(0x9999_9999)
        );
        // 22 digits that do not fit u64: an even-length id.
        assert_eq!(
            h(RAW, r#"{"block_num_or_id":"1234567890123456789012"}"#),
            Some(0x1234_5678)
        );
    }

    // A wrong id of an archived block is answered by the archive that holds
    // the block: the router must know a body names its block by id.
    #[test]
    fn a_hint_tells_an_id_from_a_number() {
        let body = |value: &str| format!(r#"{{"block_num_or_id":"{value}"}}"#);
        for ep in [BlockEndpoint::Block, BlockEndpoint::BlockHeader, RAW, BHS] {
            assert_eq!(by_id(ep, &body(SAMPLE_ID)), Some(true), "{ep:?}");
            assert_eq!(by_id(ep, r#"{"block_num_or_id":5}"#), Some(false), "{ep:?}");
            assert_eq!(by_id(ep, &body("5")), Some(false), "{ep:?}");
            // 64 digits that fit u64 are a number to fc, not an id.
            let padded = format!("{}5", "0".repeat(63));
            assert_eq!(by_id(ep, &body(&padded)), Some(false), "{ep:?}");
        }
        // Shorter ids, and digits above u64::MAX, are ids where nodeos takes
        // them as such.
        assert_eq!(by_id(RAW, &body("0a")), Some(true));
        assert_eq!(by_id(RAW, &body("99999999999999999999")), Some(true));
        assert_eq!(by_id(INFO, r#"{"block_num":"5"}"#), Some(false));
    }

    // CU2: fc reads any digit string whose value fits u64 as a number,
    // however many leading zeros it has (boost `lexical_cast`).
    #[test]
    fn zero_padded_digit_strings_are_numbers() {
        let padded = |len: usize, value: &str| format!("{}{value}", "0".repeat(len - value.len()));
        for ep in [BlockEndpoint::Block, BlockEndpoint::BlockHeader, RAW, BHS] {
            for len in [21, 22, 40, 64] {
                let body = format!(r#"{{"block_num_or_id":"{}"}}"#, padded(len, "5"));
                assert_eq!(h(ep, &body), Some(5), "{ep:?} {len}");
            }
            let max = padded(64, "18446744073709551615");
            assert_eq!(
                h(ep, &format!(r#"{{"block_num_or_id":"{max}"}}"#)),
                Some(u32::MAX),
                "{ep:?}"
            );
            // u32 truncation still applies.
            let wide = padded(30, "4294967301");
            assert_eq!(
                h(ep, &format!(r#"{{"block_num_or_id":"{wide}"}}"#)),
                Some(5),
                "{ep:?}"
            );
        }
        assert_eq!(
            h(INFO, &format!(r#"{{"block_num":"{}"}}"#, padded(64, "7"))),
            Some(7)
        );
        // A value above u64::MAX is not a number: an even-length string is an
        // id, an odd one is nothing nodeos would find.
        let over = padded(22, "18446744073709551616");
        assert_eq!(
            h(RAW, &format!(r#"{{"block_num_or_id":"{over}"}}"#)),
            Some(0x0018_4467)
        );
        let odd_over = padded(23, "18446744073709551616");
        assert_eq!(
            h(RAW, &format!(r#"{{"block_num_or_id":"{odd_over}"}}"#)),
            None
        );
        assert_eq!(h(INFO, &format!(r#"{{"block_num":"{over}"}}"#)), None);
        // Longer than 64 characters: nodeos refuses the length.
        let long = padded(65, "5");
        assert_eq!(h(RAW, &format!(r#"{{"block_num_or_id":"{long}"}}"#)), None);
    }

    // CU2: fc's depth budget runs out inside a container at level 100.
    #[test]
    fn nesting_deeper_than_fc_parses_gets_no_hint() {
        // `levels` arrays under the top-level object (level 1): the innermost
        // is at level `levels + 1`.
        let arrays = |levels: usize, inner: &str| {
            format!(
                r#"{{"block_num_or_id":"1","x":{}{inner}{}}}"#,
                "[".repeat(levels),
                "]".repeat(levels)
            )
        };
        assert_eq!(h(RAW, &arrays(98, "1")), Some(1), "level 99 holds a value");
        assert_eq!(h(RAW, &arrays(99, "")), Some(1), "level 100 may be empty");
        assert_eq!(h(RAW, &arrays(99, "1")), None, "level 100 holds a value");
        assert_eq!(h(RAW, &arrays(99, "[]")), None);
        assert_eq!(h(RAW, &arrays(120, "")), None);
        let objects = |levels: usize, inner: &str| {
            format!(
                r#"{{"x":{}{inner}{},"block_num_or_id":"1"}}"#,
                r#"{"a":"#.repeat(levels - 1) + "{",
                "}".repeat(levels)
            )
        };
        assert_eq!(h(RAW, &objects(98, r#""a":1"#)), Some(1));
        assert_eq!(h(RAW, &objects(99, "")), Some(1));
        assert_eq!(h(RAW, &objects(99, r#""a":1"#)), None);
        let shallow = format!(
            r#"{{"other":1,"block_num_or_id":"2","z":{}{}}}"#,
            "[".repeat(99),
            "]".repeat(99)
        );
        assert_eq!(h(RAW, &shallow), Some(2));
    }

    // CU2
    #[test]
    fn unusual_values_get_no_hint() {
        let odd_hex = "abc";
        let long_hex = "ab".repeat(33);
        for body in [
            r#"{"block_num_or_id":"+5"}"#.to_string(),
            r#"{"block_num_or_id":"-1"}"#.to_string(),
            r#"{"block_num_or_id":-1}"#.to_string(),
            r#"{"block_num_or_id":-0}"#.to_string(),
            r#"{"block_num_or_id":" 1"}"#.to_string(),
            r#"{"block_num_or_id":"1 "}"#.to_string(),
            r#"{"block_num_or_id":1.0}"#.to_string(),
            r#"{"block_num_or_id":1.5}"#.to_string(),
            r#"{"block_num_or_id":1e5}"#.to_string(),
            r#"{"block_num_or_id":18446744073709551616}"#.to_string(),
            r#"{"block_num_or_id":""}"#.to_string(),
            r#"{"block_num_or_id":true}"#.to_string(),
            r#"{"block_num_or_id":null}"#.to_string(),
            r#"{"block_num_or_id":[1]}"#.to_string(),
            r#"{"block_num_or_id":{"n":1}}"#.to_string(),
            format!(r#"{{"block_num_or_id":"{odd_hex}"}}"#),
            format!(r#"{{"block_num_or_id":"{long_hex}"}}"#),
            r#"{"block_num_or_id":"0x1a"}"#.to_string(),
            r#"{"block_num_or_id":"zz"}"#.to_string(),
            // 21 digits: odd length, so not an id either.
            r#"{"block_num_or_id":"123456789012345678901"}"#.to_string(),
            r#"{"other":1}"#.to_string(),
        ] {
            for ep in [BlockEndpoint::Block, BlockEndpoint::BlockHeader, RAW] {
                assert_eq!(hint(ep, body.as_bytes()), None, "{ep:?} {body:?}");
            }
        }
    }

    // CU2
    #[test]
    fn bodies_fc_reads_differently_get_no_hint() {
        for body in [
            // A backslash anywhere: fc decodes escapes its own way.
            r#"{"block_num_or_id":"\u0031"}"#,
            r#"{"block_num_or_id":"1","memo":"a\"b"}"#,
            r#"{"block_num_or_id":"1","memo":"\\"}"#,
            // Repeated top-level key: fc reads the first, serde the last.
            r#"{"block_num_or_id":"1","block_num_or_id":"2"}"#,
            r#"{"x":1,"block_num_or_id":"1","x":2}"#,
            // Not strict JSON.
            r#"{"block_num_or_id":"1",}"#,
            r#"{"block_num_or_id":"1" "x":2}"#,
            r#"{"block_num_or_id":"1"} trailing"#,
            r#"{"block_num_or_id":"1"}{}"#,
            r#"{'block_num_or_id':'1'}"#,
            r#"{block_num_or_id:1}"#,
            r#"{"block_num_or_id":01}"#,
            r#"[{"block_num_or_id":1}]"#,
            r#""1""#,
            "{\"block_num_or_id\":\"1\u{1}\"}",
            "{\"block_num_or_id\":\"1\",\"x\":\"\t\"}",
            // A non-integer number anywhere: fc parses it differently.
            r#"{"block_num_or_id":"1","x":1.5}"#,
            r#"{"block_num_or_id":"1","x":[{"y":1e3}]}"#,
            r#"{"block_num_or_id":"1","x":99999999999999999999}"#,
        ] {
            for ep in BlockEndpoint::ALL {
                assert_eq!(hint(ep, body.as_bytes()), None, "{ep:?} {body:?}");
            }
        }
        // Invalid UTF-8.
        assert_eq!(hint(RAW, b"{\"block_num_or_id\":\"1\xff\"}"), None);
        // Deep nesting beyond the parser's limit.
        let deep = format!(
            r#"{{"block_num_or_id":"1","x":{}{}}}"#,
            "[".repeat(200),
            "]".repeat(200)
        );
        assert_eq!(hint(RAW, deep.as_bytes()), None);
        // Nested repeated keys are fine: fc ignores the nested values.
        assert_eq!(
            h(RAW, r#"{"x":{"a":1,"a":2},"block_num_or_id":"9"}"#),
            Some(9)
        );
        // Negative integers elsewhere are fine.
        assert_eq!(h(RAW, r#"{"x":-5,"block_num_or_id":"9"}"#), Some(9));
    }

    // CU2
    #[test]
    fn get_block_info_reads_block_num() {
        assert_eq!(h(INFO, r#"{"block_num":5}"#), Some(5));
        assert_eq!(h(INFO, r#"{"block_num":"5"}"#), Some(5));
        assert_eq!(h(INFO, r#"{"block_num":"4294967301"}"#), Some(5));
        assert_eq!(h(INFO, r#"{"block_num":4294967301}"#), Some(5));
        for body in [
            r#"{"block_num_or_id":5}"#,
            r#"{"block_num":-5}"#,
            r#"{"block_num":"+5"}"#,
            r#"{"block_num":"ab"}"#,
            r#"{"block_num":"18446744073709551616"}"#,
            r#"{"block_num":true}"#,
            r#"{"block_num":null}"#,
            r#"{"block_num":""}"#,
            r#"{"block_num":5.0}"#,
            r#"{"block_num":5,"block_num":6}"#,
        ] {
            assert_eq!(h(INFO, body), None, "{body}");
        }
    }

    // CU2
    #[test]
    fn get_block_header_state_takes_digits_or_exactly_64_hex() {
        assert_eq!(h(BHS, r#"{"block_num_or_id":"12"}"#), Some(12));
        assert_eq!(h(BHS, r#"{"block_num_or_id":12}"#), Some(12));
        assert_eq!(
            h(BHS, &format!(r#"{{"block_num_or_id":"{SAMPLE_ID}"}}"#)),
            Some(480_385_480)
        );
        // nodeos hashes shorter or longer strings its own way: no hint.
        for value in ["0a", &SAMPLE_ID[..62], &format!("{SAMPLE_ID}00")] {
            assert_eq!(
                h(BHS, &format!(r#"{{"block_num_or_id":"{value}"}}"#)),
                None,
                "{value}"
            );
        }
        // The same short id is routable for the other endpoints.
        assert_eq!(h(RAW, r#"{"block_num_or_id":"0a"}"#), Some(0x0a00_0000));
    }
}
