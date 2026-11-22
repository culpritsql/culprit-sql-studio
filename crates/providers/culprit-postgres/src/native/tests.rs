use super::*;
use serde_json::json;

#[test]
fn native_json_oid_preserves_exact_positive_32_bit_string_identity() {
    for oid in ["1", "42", "4294967295"] {
        let row = json!({"datid":oid});
        assert_eq!(json_oid(&row, "datid").unwrap(), oid);
    }
}
#[test]
fn native_json_oid_refuses_other_json_types_zero_overflow_and_noncanonical_text() {
    assert!(json_oid(&json!({}), "datid").is_err());
    for oid in [
        json!(null),
        json!(true),
        json!(42),
        json!(42.0),
        json!(["42"]),
        json!({"value":"42"}),
        json!(""),
        json!("0"),
        json!("00"),
        json!("042"),
        json!("+42"),
        json!("-42"),
        json!(" 42"),
        json!("42 "),
        json!("0x2a"),
        json!("4294967296"),
    ] {
        assert_eq!(
            CollectionStop::from_error(&json_oid(&json!({"datid":oid.clone()}), "datid").unwrap_err()),
            CollectionStop::InvalidPage,
            "{oid}"
        );
    }
}
