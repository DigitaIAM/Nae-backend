use std::io::Error;

use json::JsonValue;
use serde::{Deserialize, Serialize};
use simsearch::SimSearch;
use uuid::Uuid;

// use crate::storage::organizations::Workspace;
use crate::{
  commutator::Application, storage::Workspaces, 
  text_search::search_engines::SimSearchEngine,
  text_search::search_engines::TantivyEngine,
  text_search::search_engines::Search,
};

pub trait SearchTrait {
  // fn load(&mut self, catalog: Vec<(Uuid, String)>);
  fn search(&self, input: &str) -> Vec<Uuid>;
}

#[derive(Clone)]
pub struct SearchEngine {
  sim: SimSearch<Uuid>,
  tan: TantivyEngine,
}

impl SearchEngine {
  pub fn new() -> Self {
    Self { 
      sim: SimSearch::new(),
      tan: TantivyEngine::new(),
    }
  }
// SIMSEARCH индексирует базу, т.к хранит индекс не на диске, а в памяти.
  pub fn load(&mut self, workspaces: Workspaces) -> Result<(), service::error::Error> {
// Итерация по всем рабочим пространствам (организациям)
    for ws in workspaces.list()? {
// Получить все записи из рабочего пространства с контекстом "drugs"
      let memories = ws.memories(vec!["drugs".to_string()]);
// Итерация по всем записям
      for mem in memories.list(None)? {
// Получить данные записи в формате JsonValue
        let jdoc = mem.json()?;
// Получить имя записи и uuid
        let name = jdoc["name"].as_str().unwrap();
        let uuid = jdoc["_uuid"].as_str().unwrap();
// Преобразовать uuid в тип Uuid
        let uuid = Uuid::parse_str(uuid).unwrap();
// Добавить запись в индекс поискового движка SimSearch
        self.sim.insert(uuid, name);
      }
    }
// Теперь у SimSearch есть в памяти индекс, который можно использовать для поиска

    Ok(())
  }
// Создать запись. Сейчас она добавляется в каталог, который я создал.
// А должна добавляться в индекс поисковых движков.
  pub fn create(&mut self, id: Uuid, text: &str) {
    self.sim.insert(id, text);
    self.tan.insert(id, text);
  }
// Изменить запись. Удалить из индекса поисковых движков и добавить новую версию.
  pub fn change(&mut self, id: Uuid, _before: &str, after: &str) {
    self.delete(&id);
    self.create(id, after);
  }
// Удалить запись. Сейчас она удаляется из каталога, который я создал.
// А должна удаляться из индекса поисковых движков.
// Как удалить запись из индекса SimSearch?
  pub fn delete(&mut self, id: &Uuid) {
// Удалить запись из каталога. Заменить на удаление из индекса поисковых движков Tantivy и SimSearch
    self.sim.delete(id);
    self.tan.delete(*id);
  }
// Поиск по индексу SimSearch
// Нужно добавить поиск по индексу Tantivy
  pub fn search(&mut self, text: &str) -> Vec<Uuid> {
    println!("-> {text}");
    let mut result_sim = self.sim.search(text);
    let mut result_tan = self.tan.search(text);
    result_sim.append(&mut result_tan);
    println!("result.len() = {}", result_sim.len());
    result_sim
  }
}
// Проверка актуальности данных в базе
pub fn process_text_search(
  app: &Application,
  ctx: &Vec<String>,
  before: &JsonValue,
  data: &JsonValue,
) -> Result<(), Error> {
  // dbg!(&ctx, &before, &data);
// Проверить, что контекст равен "drugs"
  if ctx == &vec!["drugs"] {
// Получить uuid записи
    let id = data["_uuid"].as_str().map(|data| Uuid::parse_str(data).unwrap()).unwrap();
// Получить имя записи до и после изменения
    let before_name = before["name"].as_str();
    let after_name = data["name"].as_str();
// Если имя до изменения не равно имени после изменения, то
    if let Some(before_name) = before_name {
// Если имя после изменения не пустое, то
      if let Some(after_name) = after_name {
// Если имя до изменения не равно имени после изменения, то
        if before_name == after_name {
          // IGNORE
        } else {
// Иначе изменить запись в индексе поисковых движков вызвав метод change у движков поиска
          let mut search = app.search.write().unwrap();
          search.change(id, before_name, after_name);
        }
      } else {
// Иначе удалить запись из индекса поисковых движков вызвав метод delete поискового движка
        let mut search = app.search.write().unwrap();
        search.delete(&id);
      }
    } else {
// Если имя после изменения не пустое, то
      if let Some(after_name) = after_name {
// Иначе создать запись в индексе поисковых движков вызвав метод create у обоих движков
        let mut search = app.search.write().unwrap();
        search.create(id, after_name);
      } else {
        // IGNORE
      }
    }
  }
  Ok(())
}
