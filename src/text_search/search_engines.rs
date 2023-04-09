use simsearch::{SearchOptions, SimSearch};

pub trait Search {
  fn name(&self) -> String;
  fn load(&mut self, catalog: Vec<(usize, String)>);
  fn search(&self, input: &str) -> Vec<usize>;
}

#[derive(Clone)]
pub struct SimSearchEngine {
  engine: SimSearch<usize>,
}

impl SimSearchEngine {
  pub fn new() -> Self {
    SimSearchEngine {
      engine: SimSearch::new_with(SearchOptions::new().threshold(0.9)),
    }
  }
}

impl Search for SimSearchEngine {
  fn name(&self) -> String {
    return "SimSearch".into();
  }
  fn load(&mut self, catalog: Vec<(usize, String)>) {
    catalog
      .iter()
      .for_each(|(i, data)| self.engine.insert(*i, data))
  }
  fn search(&self, input: &str) -> Vec<usize> {
    println!("-> {} = {input}", self.name());
    self.engine.search(input)
  }
}