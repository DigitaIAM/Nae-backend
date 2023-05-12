mod engine_tantivy;
mod processing;
mod tantivy_search;

pub use processing::handle_mutation;
pub use processing::SearchEngine;
pub use tantivy_search::TextSearch;
use uuid::{uuid, Uuid};

pub(crate) const UUID_NIL: Uuid = uuid!("00000000-0000-0000-0000-000000000000");
