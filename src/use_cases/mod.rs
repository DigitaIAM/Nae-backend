use crate::animo::db::AnimoDB;
use crate::animo::db::AnimoDB;
use crate::animo::memory::{ChangeTransformation, Memory};
use lazy_static::lazy_static;
use std::sync::Mutex;
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) mod csv;
pub(crate) mod uc_001;
pub(crate) mod uc_002;
pub(crate) mod uc_003;
pub(crate) mod uc_005;
pub(crate) mod uc_006;
pub(crate) mod uc_007;
pub(crate) mod uc_008;
pub(crate) mod uc_009;
pub(crate) mod uc_010;

const MAX_WRITES: usize = 17;
const PARALLEL_PROCESSING: bool = false;

lazy_static! {
  static ref BACKGROUND_TASKS: Mutex<Vec<std::thread::JoinHandle<()>>> = Mutex::new(vec![]);
}

fn wait_till(number: usize) {
  let mut tasks = BACKGROUND_TASKS.lock().unwrap();
  while tasks.len() > number {
    let h = tasks.remove(0);
    h.join().expect("Couldn't join");
    tasks.retain(|h| !h.is_finished());
  }
  tasks.retain(|h| !h.is_finished());
}

pub(crate) fn write(
  db: &AnimoDB,
  mut changes: Vec<ChangeTransformation>,
) -> Vec<ChangeTransformation> {
  // let mut bg = vec![];

  let chg = changes.clone();
  changes.clear();

  let storage = db.clone();

  if PARALLEL_PROCESSING {
    let handle = thread::spawn(move || {
      let _now = || -> u128 {
        SystemTime::now()
          .duration_since(UNIX_EPOCH)
          .expect("system time is likely incorrect")
          .as_millis()
      };

      let ts = std::time::Instant::now();
      storage.modify(chg).unwrap();
      println!("stored in {:?}", ts.elapsed());
    });
    BACKGROUND_TASKS.lock().unwrap().push(handle);
    wait_till(MAX_WRITES);
  } else {
    let _now = || -> u128 {
      SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time is likely incorrect")
        .as_millis()
    };

    let ts = std::time::Instant::now();
    storage.modify(chg).unwrap();
    println!("stored in {:?}", ts.elapsed());
  }
  changes
}

pub(crate) fn join() {
  wait_till(0);
}
