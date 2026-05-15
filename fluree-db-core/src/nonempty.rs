//! `NonEmpty<T>`: a sequence with a type-level guarantee of at least one element.

/// Construct a `NonEmpty<T>` from a comma-separated list of values.
///
/// Statically requires at least one expression — `nonempty![]` is a
/// compile error rather than a runtime panic. Useful in tests and other
/// callers where the non-empty constraint is obvious from context but
/// going through `try_from_vec(...).unwrap()` is noisy.
///
/// ```
/// use fluree_db_core::{nonempty, NonEmpty};
/// let xs: NonEmpty<i32> = nonempty![1, 2, 3];
/// assert_eq!(xs.len(), 3);
/// ```
#[macro_export]
macro_rules! nonempty {
    ($head:expr $(, $tail:expr)* $(,)?) => {
        $crate::NonEmpty::from_head_tail($head, vec![$($tail),*])
    };
}

/// Sequence with a type-level guarantee of at least one element.
///
/// The invariant is structural: `head` is always present. Downstream code
/// can rely on `first`, `iter`, etc. without empty-checks.
#[derive(Clone, Debug)]
pub struct NonEmpty<T> {
    /// First element. Always present by construction.
    pub head: T,
    /// Remaining elements (possibly empty).
    pub tail: Vec<T>,
}

impl<T> NonEmpty<T> {
    /// Construct from a single element.
    pub fn singleton(head: T) -> Self {
        Self {
            head,
            tail: Vec::new(),
        }
    }

    /// Construct from a head plus a tail of arbitrary length.
    pub fn from_head_tail(head: T, tail: Vec<T>) -> Self {
        Self { head, tail }
    }

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

    /// Iterate mutably over all elements in order, starting with the head.
    pub fn iter_mut(&mut self) -> impl DoubleEndedIterator<Item = &mut T> {
        std::iter::once(&mut self.head).chain(self.tail.iter_mut())
    }

    /// Consume self and iterate over all elements in order, starting with
    /// the head.
    pub fn into_iter(self) -> impl DoubleEndedIterator<Item = T> {
        std::iter::once(self.head).chain(self.tail)
    }

    /// Map `f` over each element, returning a new `NonEmpty<U>`. Length is
    /// preserved (the head is always mapped to a head), so the result is
    /// also non-empty by construction.
    pub fn map<U, F: FnMut(T) -> U>(self, mut f: F) -> NonEmpty<U> {
        NonEmpty {
            head: f(self.head),
            tail: self.tail.into_iter().map(f).collect(),
        }
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

    /// This method is provided for customary hygiene but, by definition, NonEmpty is never empty.
    pub fn is_empty(&self) -> bool {
        false
    }

    /// First element. Always present by construction.
    pub fn first(&self) -> &T {
        &self.head
    }

    /// Last element. Always present by construction.
    pub fn last(&self) -> &T {
        self.tail.last().unwrap_or(&self.head)
    }

    /// Append an element to the tail. The non-empty invariant is preserved
    /// trivially — the head is untouched and growing the tail only adds
    /// elements.
    pub fn push(&mut self, value: T) {
        self.tail.push(value);
    }

    /// Extend the tail with the contents of an iterator. The non-empty
    /// invariant is preserved trivially — the head is untouched.
    pub fn extend<I: IntoIterator<Item = T>>(&mut self, iter: I) {
        self.tail.extend(iter);
    }
}

impl<T> From<T> for NonEmpty<T> {
    fn from(t: T) -> Self {
        Self::singleton(t)
    }
}

/// Read-only positional access. Out-of-range indices panic just like
/// `Vec`/`slice` indexing; the invariant guarantees that index 0 always
/// succeeds.
impl<T> std::ops::Index<usize> for NonEmpty<T> {
    type Output = T;
    fn index(&self, idx: usize) -> &T {
        if idx == 0 {
            &self.head
        } else {
            &self.tail[idx - 1]
        }
    }
}

impl<'a, T> IntoIterator for &'a NonEmpty<T> {
    type Item = &'a T;
    type IntoIter = std::iter::Chain<std::iter::Once<&'a T>, std::slice::Iter<'a, T>>;
    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(&self.head).chain(self.tail.iter())
    }
}

impl<'a, T> IntoIterator for &'a mut NonEmpty<T> {
    type Item = &'a mut T;
    type IntoIter = std::iter::Chain<std::iter::Once<&'a mut T>, std::slice::IterMut<'a, T>>;
    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(&mut self.head).chain(self.tail.iter_mut())
    }
}

impl<T> IntoIterator for NonEmpty<T> {
    type Item = T;
    type IntoIter = std::iter::Chain<std::iter::Once<T>, std::vec::IntoIter<T>>;
    fn into_iter(self) -> Self::IntoIter {
        std::iter::once(self.head).chain(self.tail)
    }
}
