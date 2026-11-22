use super::*;

const POSTGRES: &str = "2026-10-11 13:48:28.887 UTC [539] ERROR:  deadlock detected
2026-10-11 13:48:28.887 UTC [539] DETAIL:  Process 539 waits for ShareLock on transaction 838; blocked by process 545.
\tProcess 545 waits for ShareLock on transaction 837; blocked by process 539.
\tProcess 539: UPDATE customers SET id = id WHERE id = 1
\tProcess 545: UPDATE orders SET id = id WHERE id = 1
2026-10-11 13:48:28.887 UTC [539] CONTEXT:  while updating tuple (0,1) in relation \"customers\"
2026-10-11 13:48:28.887 UTC [539] STATEMENT:  UPDATE customers SET id = id WHERE id = 1";

const INNODB: &str = "LATEST DETECTED DEADLOCK
2026-10-11 13:48:08 281472761716480
*** (1) TRANSACTION:
TRANSACTION 2321, ACTIVE 1 sec starting index read
MySQL thread id 74, OS thread handle 281472902020864, query id 40650 web-1 app updating
UPDATE customers SET id = id WHERE id = 1

*** (1) HOLDS THE LOCK(S):
TABLE LOCK table `shop`.`orders` trx id 2321 lock_mode X
*** (1) WAITING FOR THIS LOCK TO BE GRANTED:
TABLE LOCK table `shop`.`customers` trx id 2321 lock_mode X waiting
*** (2) TRANSACTION:
TRANSACTION 2322, ACTIVE 1 sec starting index read
MySQL thread id 75, OS thread handle 281472904257280, query id 40651 web-2 app updating
UPDATE orders SET id = id WHERE id = 1

*** (2) HOLDS THE LOCK(S):
TABLE LOCK table `shop`.`customers` trx id 2322 lock_mode X
*** (2) WAITING FOR THIS LOCK TO BE GRANTED:
TABLE LOCK table `shop`.`orders` trx id 2322 lock_mode X waiting
*** WE ROLL BACK TRANSACTION (2)
";

#[test]
fn each_native_text_format_uses_the_same_leaf_parser_without_a_database_provider() {
    let xml = crate::mssql::test_support::deadlocks::KEY_LOOKUP_XDL;
    for (input, mut expected) in [
        (xml, crate::mssql::deadlocks::parse_deadlock_xml(xml, "pasted").unwrap()),
        (POSTGRES, crate::postgres::deadlocks::parse_deadlock_log(POSTGRES)),
        (INNODB, crate::mysql::deadlocks::parse_innodb_status(INNODB, false)),
    ] {
        assert_eq!(expected.len(), 1);
        for graph in &mut expected {
            graph.source = "pasted".into();
        }
        let parsed = parse_deadlocks(input).unwrap();
        assert_eq!(parsed, expected);
        assert!(parsed[0].processes.len() >= 2);
    }
    let graph = parse_deadlocks(INNODB).unwrap().remove(0);
    assert!(graph.at.is_none(), "an imported local clock has no acquired UTC witness");
    let without_header = INNODB.strip_prefix("LATEST DETECTED DEADLOCK\n").unwrap();
    assert_eq!(parse_deadlocks(without_header).unwrap(), parse_deadlocks(INNODB).unwrap());
}

#[test]
fn unsupported_empty_unsafe_or_oversized_input_is_refused_before_publishing_graphs() {
    for input in ["", "not a deadlock", "<RingBufferTarget/>"] {
        assert!(parse_deadlocks(input).is_err());
    }
    let unsafe_xml = r#"<!DOCTYPE deadlock [<!ENTITY e SYSTEM "file:///private">]><deadlock>&e;</deadlock>"#;
    assert!(parse_deadlocks(unsafe_xml).is_err());
    assert_eq!(parse_deadlocks(&"x".repeat(MAX_TEXT + 1)).unwrap_err().message, t!("engine.deadlocks.too_large"));
}
