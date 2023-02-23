mod test_init;

use nae_backend::animo::{
    db::AnimoDB,
    memory::{Memory, ID},
  };
use nae_backend::api;
use nae_backend::memories::MemoriesInFiles;
use nae_backend::services::Services;
use nae_backend::settings::{self, Settings};
use nae_backend::storage::SOrganizations;
use nae_backend::commutator::Application;
use nae_backend::animo::Animo;
use nae_backend::animo::Topology;
use nae_backend::warehouse::store_topology::WHStoreTopology;
use nae_backend::warehouse::store_aggregation_topology::WHStoreAggregationTopology;

use utils::time::time_to_string;
use utils::json::JsonParams;

use store::
{elements::{Mode, Batch, dt, OpMutation, InternalOperation, Balance, AgregationStoreGoods, AgregationStore},
 wh_storage::WHStorage,
 error::WHError,
 check_date_store_batch::CheckDateStoreBatch,
 balance::{BalanceForGoods, BalanceDelta},
 date_type_store_batch_id::DateTypeStoreBatchId,
 store_date_type_batch_id::StoreDateTypeBatchId};

use std::{io, thread, time::Duration};
use std::sync::Arc;
use actix_web::{http::header::ContentType, test, web, App};
use futures::TryFutureExt;
use json::{object, JsonValue};
use rocksdb::{ColumnFamilyDescriptor, Options, IteratorMode};
use serde_json::json;
use tempfile::{TempDir, tempdir};
use uuid::Uuid;

const G1: Uuid = Uuid::from_u128(1);
const G2: Uuid = Uuid::from_u128(2);
const G3: Uuid = Uuid::from_u128(3);


