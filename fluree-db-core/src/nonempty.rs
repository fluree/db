//! `NonEmpty<T>`: a sequence with a type-level guarantee of at least one element.

/// Sequence with a type-level guarantee of at least one element. The
/// invariant is structural — `head` is always present — so downstream code
/// can rely on `first`, `iter`, etc. without empty-checks. Constructed only
/// at validation boundaries.
#[derive(Clone, Debug)]
pub struct NonEmpty<T> {
    pub head: T,
    pub tail: Vec<T>,
}

impl<T> NonEmpty<T> {
    /// Construct from a `Vec`, returning `None` if the input is empty.
    pub fn try_from_vec(v: Vec<T>) -> Option<Self> {
        let mut iter = v.into_iter();
        let head = iter.next()?;
        Some(Self {
            head,
            tail: iter.collect(),
        })
    }

    /// Iterate over all elements in order, starting with the head.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> {
        std::iter::once(&self.head).chain(self.tail.iter())
    }

    /// Convert into a (necessarily non-empty) `Vec`.
    pub fn into_vec(self) -> Vec<T> {
        let mut v = Vec::with_capacity(1 + self.tail.len());
        v.push(self.head);
        v.extend(self.tail);
        v
    }

    /// Number of elements, always ≥ 1.
    pub fn len(&self) -> usize {
        1 + self.tail.len()
    }
}

impl<T> From<T> for NonEmpty<T> {
    fn from(t: T) -> Self {
        Self {
            head: t,
            tail: Vec::new(),
        }
    }
}
