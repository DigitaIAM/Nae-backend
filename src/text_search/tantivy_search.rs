use std::collections::HashSet;
use std::path::Path;
use std::sync::{Arc, RwLock};
use tantivy::collector::TopDocs;
use tantivy::query::{FuzzyTermQuery, QueryParser, RegexQuery};
use tantivy::schema::{Field, STRING};
use tantivy::{
  schema::{Schema, STORED, TEXT},
  DocAddress, Document, Index, IndexReader, IndexWriter, Score, Term,
};

use crate::animo::db::Snapshot;
use crate::animo::error::{convert, DBError};
use crate::animo::memory::{ChangeTransformation, Context, Value};
use crate::animo::shared::*;
use crate::animo::Txn;

use values::ID;

pub struct TextSearch {
  index: Index,
  writer: Arc<RwLock<IndexWriter>>,
  reader: IndexReader,
  field_key: Field,
  field_text: Field,
  field_string: Field,
}

impl TextSearch {
  pub(crate) fn schema() -> (Schema, Field, Field, Field) {
    let mut schema_builder = Schema::builder();
    let field_key = schema_builder.add_text_field("key", TEXT | STORED);
    let field_text = schema_builder.add_text_field("text", TEXT);
    let field_string = schema_builder.add_text_field("string", STRING);
    (schema_builder.build(), field_key, field_text, field_string)
  }

  pub(crate) fn open<P: AsRef<Path>>(path: P) -> Result<Self, DBError> {
    let directory = tantivy::directory::MmapDirectory::open(path).unwrap();

    let (schema, field_key, field_text, field_string) = TextSearch::schema();

    let index = Index::open_or_create(directory, schema).unwrap();

    let writer = index.writer(50_000_000).map_err(convert)?;
    let writer = Arc::new(RwLock::new(writer));

    let reader = index.reader().map_err(convert)?;

    Ok(TextSearch { index, writer, reader, field_key, field_text, field_string })
  }

  fn depends_on(&self) -> Vec<ID> {
    todo!()
  }

  fn on_mutation(&self, _tx: &mut Txn, _changes: Vec<ChangeTransformation>) -> Result<(), DBError> {
    todo!()
  }

  pub(crate) fn modification(
    &self,
    _s: &Snapshot,
    tx: Txn,
    changes: &Vec<ChangeTransformation>,
  ) -> Result<(), DBError> {
    let mut to_index = HashSet::with_capacity(changes.len());

    for change in changes {
      if change.zone == *CAN_BUY_FROM {
        if change.context.0.len() > 2 {
          let _supplier = change.context.0[0];
          let goods = change.context.0[1];

          to_index.insert(goods);
        }
      }
    }

    let mut indexing = Vec::with_capacity(to_index.len());

    for goods in to_index {
      match tx.resolve(*DESC, &Context(vec![goods]), *LABEL)? {
        Some(change) => {
          match change.into_after {
            Value::Nothing => {
              todo!()
              // w.delete_term(doc!(
              //   context: "test",
              //   text: str
              // ));
            },
            Value::String(str) => {
              let key = "test";
              let key_term = Term::from_field_text(self.field_key, key);

              indexing.push((key, key_term, str));
            },
            _ => {},
          }
        },
        None => {},
      }
    }

    if !indexing.is_empty() {
      {
        // it require write lock only for commit,
        let mut w = self.writer.write().unwrap();

        for (id, key, str) in indexing {
          println!("indexing: {:?}", str);

          w.delete_term(key);

          let mut document = Document::default();
          document.add_field_value(self.field_key, id);
          document.add_field_value(self.field_text, str);
          w.add_document(document);
        }

        w.commit().map_err(convert)?;
      }
      self.reader.reload().map_err(convert)?;
    }

    Ok(())
  }

  fn search(&self, str: &str) -> Result<(), DBError> {
    let schema = self.index.schema();

    let index = &self.index;
    let searcher = self.reader.searcher();

    let query_parser = QueryParser::for_index(index, vec![self.field_text]);
    let query = query_parser.parse_query(str).map_err(convert)?;

    let top_docs: Vec<(Score, DocAddress)> =
      searcher.search(&query, &TopDocs::with_limit(10)).map_err(convert)?;

    println!("1st:");
    for (_score, doc_address) in top_docs {
      let retrieved_doc = searcher.doc(doc_address).map_err(convert)?;
      println!("{}", schema.to_json(&retrieved_doc));
    }

    let q = str.to_lowercase();

    let term = Term::from_field_text(self.field_text, q.as_str());
    let query = FuzzyTermQuery::new(term, 1, true);

    let top_docs: Vec<(Score, DocAddress)> =
      searcher.search(&query, &TopDocs::with_limit(10)).map_err(convert)?;

    println!("2nd:");
    for (_score, doc_address) in top_docs {
      let retrieved_doc = searcher.doc(doc_address).map_err(convert)?;
      println!("{}", schema.to_json(&retrieved_doc));
    }

    let q = format!("({})(.+)", str);
    let query = RegexQuery::from_pattern(q.as_str(), self.field_text)
      .map_err(|e| DBError::from(e.to_string()))?;

    let top_docs: Vec<(Score, DocAddress)> =
      searcher.search(&query, &TopDocs::with_limit(10)).map_err(convert)?;

    println!("3rd:");
    for (_score, doc_address) in top_docs {
      let retrieved_doc = searcher.doc(doc_address).map_err(convert)?;
      println!("{}", schema.to_json(&retrieved_doc));
    }

    println!("-----");

    Ok(())
  }
}

#[cfg(test)]
mod tests {
  use crate::animo::{memory::create, shared::*};
  use crate::warehouse::test_util::init;
  use crate::{Memory, Value};
  use std::thread::Thread;

  use super::*;

  #[test]
  fn test_search() {
    let (tmp_dir, settings, db) = init();

    let mut changes = vec![];

    let schneider_electric = ID::from("schneider-electric|company");
    let goods1 = ID::from("schneider-electric|goods|1");
    let goods2 = ID::from("schneider-electric|goods|2");
    let goods3 = ID::from("schneider-electric|goods|3");

    println!("goods1 {:?}", goods1);
    println!("goods2 {:?}", goods2);
    println!("goods3 {:?}", goods3);

    changes.extend(create(
      *DESC,
      goods1,
      vec![
        (*REFERENCE, "IMDIFL12LMC".into()),
        (*LABEL, "Локализатор поврежд IFL12 24-48В Modbus".into()),
      ],
    ));
    changes.extend(create(
      *CAN_BUY_FROM,
      schneider_electric,
      vec![(
        goods1,
        vec![(*PRICE, vec![(*NUMBER, 3.into()), (*CURRENCY, Value::from(*EUR))].into())].into(),
      )],
    ));

    println!("{:?}", changes);

    changes.extend(create(
      *DESC,
      goods2,
      vec![
        (*REFERENCE, "METSECTV35010".into()),
        (*LABEL, "Трансформатор тока неразъемный 3в1 RJ45 35мм 100А:1/3В".into()),
      ],
    ));
    changes.extend(create(
      *CAN_BUY_FROM,
      schneider_electric,
      vec![(
        goods2,
        vec![(*PRICE, vec![(*NUMBER, 5.into()), (*CURRENCY, Value::from(*EUR))].into())].into(),
      )],
    ));

    changes.extend(create(
      *DESC,
      goods3,
      vec![
        (*REFERENCE, "IMDIFL12L".into()),
        (*LABEL, "Локализатор повреждения изоляции IFL12, 24-48В".into()),
      ],
    ));
    changes.extend(create(
      *CAN_BUY_FROM,
      schneider_electric,
      vec![(
        goods3,
        vec![(*PRICE, vec![(*NUMBER, 7.into()), (*CURRENCY, Value::from(*EUR))].into())].into(),
      )],
    ));

    db.modify(changes).unwrap();
    // std::thread::sleep(std::time::Duration::from_millis(100));

    for text in vec!["Локализатор повреждения", "локализатор", "лока", "Трансформатор то"]
    {
      println!("{:?}", text);
      db.text_search.search(text).unwrap();
    }
  }
}
