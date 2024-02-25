use store::check_date_store_batch::CheckDateStoreBatch;
use store::elements::{dt, Batch};
use uuid::Uuid;

#[test]
fn store_test_key_to_data() {
  let date1 = dt("2022-12-15").unwrap();
  let storage1 = Uuid::from_u128(201);
  let goods1 = Uuid::from_u128(101);
  let batch = Batch { id: Uuid::from_u128(102), date: date1 };

  let key: Vec<u8> = []
    .iter()
    .chain((date1.timestamp() as u64).to_be_bytes().iter())
    .chain(storage1.as_bytes().iter())
    .chain(goods1.as_bytes().iter())
    .chain((batch.date.timestamp() as u64).to_be_bytes().iter())
    .chain(batch.id.as_bytes().iter())
    .map(|b| *b)
    .collect();

  let (d, s, g, b) = CheckDateStoreBatch::key_to_data(key).unwrap();

  // println!("{d:?}, {s:?}, {g:?}, {b:?}");

  assert_eq!(date1, d);
  assert_eq!(storage1, s);
  assert_eq!(goods1, g);
  assert_eq!(batch, b);
}
