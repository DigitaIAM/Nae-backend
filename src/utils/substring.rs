use std::ops::{Bound, RangeBounds};

pub trait StringUtils {
  fn substring(&self, start: usize, len: usize) -> &str;
  fn slice(&self, range: impl RangeBounds<usize>) -> &str;
}

impl StringUtils for str {
  fn substring(&self, start: usize, len: usize) -> &str {
    let mut char_pos = 0;
    let mut byte_start = 0;
    let mut it = self.chars();
    loop {
      if char_pos == start {
        break;
      }
      if let Some(c) = it.next() {
        char_pos += 1;
        byte_start += c.len_utf8();
      } else {
        break;
      }
    }
    char_pos = 0;
    let mut byte_end = byte_start;
    loop {
      if char_pos == len {
        break;
      }
      if let Some(c) = it.next() {
        char_pos += 1;
        byte_end += c.len_utf8();
      } else {
        break;
      }
    }
    &self[byte_start..byte_end]
  }

  fn slice(&self, range: impl RangeBounds<usize>) -> &str {
    let start = match range.start_bound() {
      Bound::Included(bound) | Bound::Excluded(bound) => *bound,
      Bound::Unbounded => 0,
    };
    let len = match range.end_bound() {
      Bound::Included(bound) => *bound + 1,
      Bound::Excluded(bound) => *bound,
      Bound::Unbounded => self.len(),
    } - start;
    self.substring(start, len)
  }
}

#[test]
fn test_string_utils() {
  let s = "abcdèfghij";
  // All three statements should print:
  // "abcdè, abcdèfghij, dèfgh, dèfghij."
  println!(
    "{}, {}, {}, {}.",
    s.substring(0, 5),
    s.substring(0, 50),
    s.substring(3, 5),
    s.substring(3, 50)
  );
  println!("{}, {}, {}, {}.", s.slice(..5), s.slice(..50), s.slice(3..8), s.slice(3..));
  println!("{}, {}, {}, {}.", s.slice(..=4), s.slice(..=49), s.slice(3..=7), s.slice(3..));
}
