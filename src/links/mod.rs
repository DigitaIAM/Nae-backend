use crate::links::links_index::LinksIndex;

pub mod links_index;

#[cfg(test)]
mod test_save_links;

pub trait GetLinks {
  fn links(&self) -> LinksIndex;
}
