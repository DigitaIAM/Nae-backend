use store::error::WHError;
use tempfile::TempDir;
use store::wh_storage::WHStorage;
use store::elements::{dt, Batch, OpMutation, InternalOperation, Mode};
use uuid::Uuid;
use store::date_type_store_batch_id::DateTypeStoreBatchId;
use store::store_date_type_batch_id::StoreDateTypeBatchId;
use rocksdb::IteratorMode;
use rust_decimal::Decimal; // TODO change to another Decimal
use core::str::FromStr;

const G1: Uuid = Uuid::from_u128(1);
const G2: Uuid = Uuid::from_u128(2);
const G3: Uuid = Uuid::from_u128(3);

#[test]
fn store_test_op_iter() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_op_iter");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-11-01").expect("test_op_iter");
    let w1 = Uuid::new_v4();
    let party = Batch { id: Uuid::new_v4(), date: start_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);
    let id3 = Uuid::from_u128(103);
    let id4 = Uuid::from_u128(104);

    let ops = vec![
        OpMutation::new(
            id3,
            start_d,
            w1,
            None,
            G2,
            party.clone(),
            None,
            Some(InternalOperation::Receive(1.into(), 1000.into())),
        ),
        OpMutation::new(
            id4,
            start_d,
            w1,
            None,
            G2,
            party.clone(),
            None,
            Some(InternalOperation::Receive(1.into(), 1000.into())),
        ),
        OpMutation::new(
            id1,
            start_d,
            w1,
            None,
            G3,
            party.clone(),
            None,
            Some(InternalOperation::Issue(Decimal::from_str("0.5").unwrap(), 1500.into(), Mode::Manual)),
        ),
        OpMutation::new(
            id2,
            start_d,
            w1,
            None,
            G3,
            party.clone(),
            None,
            Some(InternalOperation::Issue(Decimal::from_str("0.5").unwrap(), 1500.into(), Mode::Manual)),
        ),
    ];

    db.record_ops(&ops).unwrap();

    let cf1 = db.db.cf_handle(DateTypeStoreBatchId::cf_name()).unwrap();
    let cf2 = db.db.cf_handle(StoreDateTypeBatchId::cf_name()).unwrap();

    let iter1 = db.db.iterator_cf(&cf1, IteratorMode::Start);
    let iter2 = db.db.iterator_cf(&cf2, IteratorMode::Start);

    let mut res1: Vec<String> = Vec::new();
    let mut res2: Vec<String> = Vec::new();

    for item in iter1 {
        let (_, v) = item.unwrap();
        let str = String::from_utf8_lossy(&v).to_string();

        // println!("{str:?}");

        res1.push(str);
    }

    for item in iter2 {
        let (_, v) = item.unwrap();
        let str = String::from_utf8_lossy(&v).to_string();

        // println!("{str:?}");

        res2.push(str);
    }

    for i in 0..res1.len() {
        assert_eq!(res2[i], res1[i]);
    }

    tmp_dir.close().expect("Can't remove tmp dir in test_op_iter");
}