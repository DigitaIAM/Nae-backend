use uuid::Uuid;
use tempfile::TempDir;
use store::error::WHError;
use store::wh_storage::WHStorage;
use store::elements::{dt, Batch, OpMutation, InternalOperation, Mode, AgregationStoreGoods};
use store::balance::{BalanceForGoods, BalanceDelta};

const G1: Uuid = Uuid::from_u128(1);
const G2: Uuid = Uuid::from_u128(2);

#[test]
fn store_test_get_aggregations_without_checkpoints() -> Result<(), WHError> {
    let tmp_dir = TempDir::new().expect("Can't create tmp dir in test_get_wh_balance");

    let wh = WHStorage::open(&tmp_dir.path()).unwrap();
    let mut db = wh.database;

    let op_d = dt("2022-10-10")?;
    let check_d = dt("2022-10-11")?;
    let w1 = Uuid::new_v4();
    let doc1 = Batch { id: Uuid::new_v4(), date: dt("2022-10-09")? };
    let doc2 = Batch { id: Uuid::new_v4(), date: op_d };

    let id1 = Uuid::from_u128(101);
    let id2 = Uuid::from_u128(102);
    let id3 = Uuid::from_u128(103);
    let id4 = Uuid::from_u128(104);

    let ops = vec![
        OpMutation::new(
            id1,
            op_d,
            w1,
            None,
            G1,
            doc1.clone(),
            None,
            Some(InternalOperation::Receive(3.into(), 3000.into())),
        ),
        OpMutation::new(
            id2,
            op_d,
            w1,
            None,
            G1,
            doc1.clone(),
            None,
            Some(InternalOperation::Issue(1.into(), 1000.into(), Mode::Manual)),
        ),
        OpMutation::new(
            id3,
            op_d,
            w1,
            None,
            G2,
            doc2.clone(),
            None,
            Some(InternalOperation::Issue(2.into(), 2000.into(), Mode::Manual)),
        ),
        OpMutation::new(
            id4,
            op_d,
            w1,
            None,
            G2,
            doc2.clone(),
            None,
            Some(InternalOperation::Receive(2.into(), 2000.into())),
        ),
    ];

    db.record_ops(&ops).unwrap();

    let agregations = vec![
        AgregationStoreGoods {
            store: Some(w1),
            goods: Some(G1),
            batch: Some(doc1.clone()),
            open_balance: BalanceForGoods::default(),
            receive: BalanceDelta { qty: 3.into(), cost: 3000.into() },
            issue: BalanceDelta { qty: (-1).into(), cost: (-1000).into() },
            close_balance: BalanceForGoods { qty: 2.into(), cost: 2000.into() },
        },
        AgregationStoreGoods {
            store: Some(w1),
            goods: Some(G2),
            batch: Some(doc2.clone()),
            open_balance: BalanceForGoods::default(),
            receive: BalanceDelta { qty: 2.into(), cost: 2000.into() },
            issue: BalanceDelta { qty: (-2).into(), cost: (-2000).into() },
            close_balance: BalanceForGoods::default(),
        },
    ];

    let res = db.get_report(w1, op_d, check_d)?;

    assert_eq!(agregations, res.items.1);

    tmp_dir.close().expect("Can't close tmp dir in store_test_get_wh_balance");

    Ok(())
}