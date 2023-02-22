use store::error::WHError;
use tempfile::TempDir;
use store::wh_storage::WHStorage;
use store::elements::{dt, Batch, OpMutation, InternalOperation, Mode, AgregationStore, AgregationStoreGoods};
use uuid::Uuid;
use store::balance::{BalanceForGoods, BalanceDelta};

const G1: Uuid = Uuid::from_u128(1);


#[test]
fn store_test_issue_op_none() {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_issue_op_none");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let start_d = dt("2022-10-10").unwrap();
    let end_d = dt("2022-10-11").unwrap();
    let w1 = Uuid::new_v4();

    let doc = Batch { id: Uuid::new_v4(), date: start_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);
    let id3 = Uuid::from_u128(103);

    let ops = vec![
        OpMutation::new(
            id1,
            start_d,
            w1,
            None,
            G1,
            doc.clone(),
            None,
            Some(InternalOperation::Receive(3.into(), 10.into())),
        ),
        // КОРРЕКТНАЯ ОПЕРАЦИЯ С ДВУМЯ NONE?
        OpMutation::new(id3, start_d, w1, None, G1, doc.clone(), None, None),
    ];

    db.record_ops(&ops).unwrap();

    let res = db.get_report(w1, start_d, end_d).unwrap();

    // println!("REPORT: {res:#?}");

    let agr = AgregationStoreGoods {
        store: Some(w1),
        goods: Some(G1),
        batch: Some(doc.clone()),
        open_balance: BalanceForGoods::default(),
        receive: BalanceDelta { qty: 3.into(), cost: 10.into() },
        issue: BalanceDelta { qty: 0.into(), cost: 0.into() },
        close_balance: BalanceForGoods { qty: 3.into(), cost: 10.into() },
    };

    assert_eq!(agr, res.items.1[0]);

    tmp_dir.close().expect("Can't remove tmp dir in test_issue_op_none");
}