//! A list member's place: which list of its graph, subject and predicate it
//! is in, and its position in that list.
//!
//! A position is at most `i32::MAX`. The index and the commit format carry
//! both as one `u64`, the list in the high 32 bits and the position in the
//! low 32, so a property's list members sort by list, then position, and a
//! position written before lists had identities (list 0) reads unchanged.
//! [`NONE`] is the place of a value that is no list member.
//!
//! ## Nested lists
//!
//! A list holding lists is a tree whose leaves are the values, each still a
//! value of the owning graph, subject and predicate. Nothing is stored for an
//! inner list: its shape lives in the leaves' places. The tree's list id has
//! [`NESTED`] set, and each leaf's position packs its [`Leaf`]:
//!
//! ```text
//! list:     NESTED | id (31)
//! position: 0 | pos (23) | depth (4) | opens (4)
//! ```
//!
//! `pos` orders the leaves across the tree, `depth` counts the lists a leaf
//! sits in below the top-level one, and `opens` how many of those begin right
//! before it. The lists that close before a leaf follow from its neighbour:
//! the leaf shares `depth − opens` of its lists with the one before it. An
//! empty inner list is the value `rdf:nil`, so every tree decodes one way.
//! A flat list's position is its member's index, and its members are leaves
//! at depth 0.

/// The packed place of a value that is no list member.
pub const NONE: u64 = u64::MAX;

/// A list member's place, unpacked: its list, then its position.
pub type ListPlace = (u32, i32);

/// The bit of a list id that marks a nested list, whose positions pack
/// [`Leaf`]s. A flat list's id never has it.
pub const NESTED: u32 = 1 << 31;

/// Lists one graph, subject and predicate can hold: ids below [`NESTED`].
pub const MAX_LISTS: u32 = NESTED;

/// Lists a leaf can sit in below its tree's top-level list.
pub const MAX_DEPTH: u8 = 15;

/// Leaves one nested list can hold.
pub const MAX_LEAVES: u32 = 1 << 23;

/// `list` and `position` as one `u64`. `position` must not be negative.
#[inline]
pub const fn pack(list: u32, position: i32) -> u64 {
    debug_assert!(position >= 0, "a list position is never negative");
    ((list as u64) << 32) | (position as u32 as u64)
}

/// The list and position `packed` holds: `None` for [`NONE`], and for a
/// position past `i32::MAX`, which [`pack`] never writes.
#[inline]
pub fn unpack(packed: u64) -> Option<(u32, i32)> {
    if packed == NONE {
        return None;
    }
    let position = i32::try_from(packed & 0xffff_ffff).ok()?;
    Some(((packed >> 32) as u32, position))
}

/// Whether `list` is a nested list's id.
#[inline]
pub const fn is_nested(list: u32) -> bool {
    list & NESTED != 0
}

/// `list` without [`NESTED`]: the number flat lists and trees share.
#[inline]
pub const fn id_of(list: u32) -> u32 {
    list & !NESTED
}

/// A list member as a leaf of its list's tree: its order, how many lists it
/// sits in below the top-level one, and how many of those begin right before
/// it. A flat list's members are leaves at depth 0.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Leaf {
    pub pos: u32,
    pub depth: u8,
    pub opens: u8,
}

impl Leaf {
    /// The leaf at `position` in `list`.
    #[inline]
    pub const fn of(list: u32, position: i32) -> Leaf {
        let bits = position as u32;
        if is_nested(list) {
            Leaf {
                pos: (bits >> 8) & (MAX_LEAVES - 1),
                depth: ((bits >> 4) & 0xf) as u8,
                opens: (bits & 0xf) as u8,
            }
        } else {
            Leaf {
                pos: bits,
                depth: 0,
                opens: 0,
            }
        }
    }

    /// The position that stores this leaf in a nested list. `pos` must be
    /// below [`MAX_LEAVES`], and `depth` and `opens` at most [`MAX_DEPTH`].
    #[inline]
    pub const fn nested_position(self) -> i32 {
        debug_assert!(self.pos < MAX_LEAVES && self.depth <= MAX_DEPTH);
        debug_assert!(self.opens <= self.depth);
        ((self.pos << 8) | ((self.depth as u32) << 4) | self.opens as u32) as i32
    }

    /// How many of its lists this leaf shares with the one before it.
    #[inline]
    pub const fn shared(self) -> u8 {
        self.depth.saturating_sub(self.opens)
    }

    /// Its shape, what two lists' leaves must agree on to be one list.
    #[inline]
    pub const fn shape(self) -> (u8, u8) {
        (self.depth, self.opens)
    }
}

/// An item of a list: a value, or a list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Item<T> {
    Value(T),
    List(Vec<Item<T>>),
}

impl<T> Item<T> {
    /// The value, when the item is one.
    pub fn value(&self) -> Option<&T> {
        match self {
            Item::Value(v) => Some(v),
            Item::List(_) => None,
        }
    }
}

/// Whether any of `items` is a list.
pub fn holds_list<T>(items: &[Item<T>]) -> bool {
    items.iter().any(|item| matches!(item, Item::List(_)))
}

/// Why a list cannot be stored as a tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeLimit {
    /// A value sits in more than [`MAX_DEPTH`] lists below the top level.
    Depth,
    /// The list holds more than [`MAX_LEAVES`] values.
    Leaves,
}

/// `items` as the leaves that store them, in order. An empty inner list is
/// the value `nil()`, the list `rdf:nil`.
pub fn leaves<T>(items: Vec<Item<T>>, nil: impl Fn() -> T) -> Result<Vec<(T, Leaf)>, TreeLimit> {
    fn walk<T>(
        items: Vec<Item<T>>,
        depth: u8,
        opens: &mut u8,
        nil: &impl Fn() -> T,
        out: &mut Vec<(T, Leaf)>,
    ) -> Result<(), TreeLimit> {
        for item in items {
            match item {
                Item::List(inner) if inner.is_empty() => push(nil(), depth, opens, out)?,
                Item::List(inner) => {
                    if depth == MAX_DEPTH {
                        return Err(TreeLimit::Depth);
                    }
                    *opens += 1;
                    walk(inner, depth + 1, opens, nil, out)?;
                }
                Item::Value(v) => push(v, depth, opens, out)?,
            }
        }
        Ok(())
    }
    fn push<T>(v: T, depth: u8, opens: &mut u8, out: &mut Vec<(T, Leaf)>) -> Result<(), TreeLimit> {
        let pos = u32::try_from(out.len()).map_err(|_| TreeLimit::Leaves)?;
        if pos >= MAX_LEAVES {
            return Err(TreeLimit::Leaves);
        }
        out.push((
            v,
            Leaf {
                pos,
                depth,
                opens: std::mem::take(opens),
            },
        ));
        Ok(())
    }
    let mut out = Vec::new();
    walk(items, 0, &mut 0, &nil, &mut out)?;
    Ok(out)
}

/// The items `leaves`, in order, store.
pub fn items<T>(leaves: impl IntoIterator<Item = (T, Leaf)>) -> Vec<Item<T>> {
    kept_items(leaves.into_iter().map(|(v, leaf)| (Some(v), leaf)))
}

/// The items `leaves`, in order, store, with each `None` hidden: the items
/// read as if the hidden leaves were deleted. A leaf shares with the kept
/// leaf before it the fewest lists any leaf between them shares with its
/// neighbour, and an inner list left with no leaf is gone.
///
/// Bits no writer stores — a leaf sharing more lists than the one before it
/// sits in — read as the most it can share, never as a panic.
pub fn kept_items<T>(leaves: impl IntoIterator<Item = (Option<T>, Leaf)>) -> Vec<Item<T>> {
    let mut stack: Vec<Vec<Item<T>>> = vec![Vec::new()];
    let mut run_shared = u8::MAX;
    for (value, leaf) in leaves {
        run_shared = run_shared.min(leaf.shared());
        let Some(value) = value else {
            continue;
        };
        let depth = usize::from(leaf.depth.min(MAX_DEPTH));
        let shared = usize::from(run_shared).min(stack.len() - 1).min(depth);
        run_shared = u8::MAX;
        while stack.len() - 1 > shared {
            let inner = stack.pop().expect("an inner list");
            stack
                .last_mut()
                .expect("the top-level list")
                .push(Item::List(inner));
        }
        while stack.len() - 1 < depth {
            stack.push(Vec::new());
        }
        stack
            .last_mut()
            .expect("the leaf's list")
            .push(Item::Value(value));
    }
    while stack.len() > 1 {
        let inner = stack.pop().expect("an inner list");
        stack
            .last_mut()
            .expect("the top-level list")
            .push(Item::List(inner));
    }
    stack.pop().expect("the top-level list")
}

/// The leaves of a tree that change shape when the leaves `deleted` says go:
/// the leaf after each run of deleted ones, which then shares with the leaf
/// before the run the fewest lists any leaf of the run, or it, shares with
/// its neighbour. `leaves` are the tree's, in order; each change is the
/// leaf's index there and its new shape.
pub fn repair(leaves: &[Leaf], deleted: impl Fn(usize) -> bool) -> Vec<(usize, Leaf)> {
    let mut out = Vec::new();
    let mut run_shared: Option<u8> = None;
    for (k, leaf) in leaves.iter().enumerate() {
        if deleted(k) {
            run_shared = Some(run_shared.unwrap_or(u8::MAX).min(leaf.shared()));
            continue;
        }
        if let Some(run) = run_shared.take() {
            let shared = run.min(leaf.shared());
            let repaired = Leaf {
                opens: leaf.depth - shared,
                ..*leaf
            };
            if repaired != *leaf {
                out.push((k, repaired));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_place_packs_list_high_and_position_low() {
        assert_eq!(pack(0, 7), 7, "list 0 is the position alone");
        assert_eq!(unpack(pack(3, 7)), Some((3, 7)));
        assert_eq!(unpack(pack(u32::MAX, i32::MAX)), Some((u32::MAX, i32::MAX)));
        assert!(
            pack(u32::MAX, i32::MAX) < NONE,
            "every place sorts below none"
        );
        assert!(
            pack(0, i32::MAX) < pack(1, 0),
            "lists sort before positions"
        );
        assert_eq!(unpack(NONE), None);
        assert_eq!(
            unpack(u64::from(u32::MAX)),
            None,
            "a position past i32::MAX"
        );
    }

    fn v(x: u32) -> Item<u32> {
        Item::Value(x)
    }

    fn l(items: Vec<Item<u32>>) -> Item<u32> {
        Item::List(items)
    }

    const NIL: u32 = u32::MAX;

    #[test]
    fn a_tree_packs_its_shape_into_its_leaves() {
        // ((1 2) 3)
        let tree = vec![l(vec![v(1), v(2)]), v(3)];
        let leaves = leaves(tree.clone(), || NIL).unwrap();
        let shapes: Vec<_> = leaves.iter().map(|(x, l)| (*x, l.pos, l.shape())).collect();
        assert_eq!(shapes, [(1, 0, (1, 1)), (2, 1, (1, 0)), (3, 2, (0, 0))]);
        let positions: Vec<i32> = leaves.iter().map(|(_, l)| l.nested_position()).collect();
        assert_eq!(positions, [0x11, 0x110, 0x200]);
        for ((_, leaf), position) in leaves.iter().zip(&positions) {
            assert_eq!(Leaf::of(NESTED | 4, *position), *leaf);
        }
        assert_eq!(items(leaves), tree);
    }

    #[test]
    fn opens_tell_apart_what_depth_alone_cannot() {
        // ((1 2)) and ((1) (2)): leaf 2 is at depth 1 in both.
        let one = leaves(vec![l(vec![v(1), v(2)])], || NIL).unwrap();
        let two = leaves(vec![l(vec![v(1)]), l(vec![v(2)])], || NIL).unwrap();
        assert_eq!(one[1].1.shape(), (1, 0));
        assert_eq!(two[1].1.shape(), (1, 1));
    }

    #[test]
    fn an_empty_inner_list_is_rdf_nil() {
        let leaves = leaves(vec![l(vec![]), l(vec![l(vec![])])], || NIL).unwrap();
        let shapes: Vec<_> = leaves.iter().map(|(x, l)| (*x, l.shape())).collect();
        assert_eq!(shapes, [(NIL, (0, 0)), (NIL, (1, 1))]);
    }

    #[test]
    fn a_flat_lists_position_is_its_members_index() {
        assert_eq!(
            Leaf::of(3, 7),
            Leaf {
                pos: 7,
                depth: 0,
                opens: 0
            }
        );
        assert_eq!(id_of(NESTED | 9), 9);
        assert!(is_nested(NESTED) && !is_nested(MAX_LISTS - 1));
    }

    #[test]
    fn a_tree_past_its_limits_is_refused() {
        let mut deep = v(1);
        for _ in 0..=MAX_DEPTH {
            deep = l(vec![deep]);
        }
        assert_eq!(leaves(vec![deep], || NIL), Err(TreeLimit::Depth));
        let mut fits = v(1);
        for _ in 0..MAX_DEPTH {
            fits = l(vec![fits]);
        }
        let leaves = leaves(vec![fits], || NIL).unwrap();
        assert_eq!(leaves[0].1.shape(), (MAX_DEPTH, MAX_DEPTH));
    }

    /// Every list over `leaves` values with no empty inner list and no
    /// value deeper than `max_depth` below the top level.
    fn trees(leaves: u32, max_depth: u8) -> Vec<Vec<Item<u32>>> {
        fn seqs(items: &[u32], depth: u8, max_depth: u8) -> Vec<Vec<Item<u32>>> {
            let Some((&first, rest)) = items.split_first() else {
                return vec![Vec::new()];
            };
            let mut out = Vec::new();
            for tail in seqs(rest, depth, max_depth) {
                let mut seq = vec![v(first)];
                seq.extend(tail);
                out.push(seq);
            }
            if depth < max_depth {
                for k in 1..=items.len() {
                    for inner in seqs(&items[..k], depth + 1, max_depth) {
                        for tail in seqs(&items[k..], depth, max_depth) {
                            let mut seq = vec![l(inner.clone())];
                            seq.extend(tail);
                            out.push(seq);
                        }
                    }
                }
            }
            out
        }
        let items: Vec<u32> = (0..leaves).collect();
        seqs(&items, 0, max_depth)
    }

    fn prune(items: &[Item<u32>], gone: &dyn Fn(u32) -> bool) -> Vec<Item<u32>> {
        items
            .iter()
            .filter_map(|item| match item {
                Item::Value(x) => (!gone(*x)).then_some(v(*x)),
                Item::List(inner) => {
                    let inner = prune(inner, gone);
                    (!inner.is_empty()).then_some(l(inner))
                }
            })
            .collect()
    }

    /// Every tree of at most 5 values and depth 3, with every set of its
    /// values deleted: hiding them and repairing the leaves after them both
    /// read as the tree without them.
    #[test]
    fn deleting_or_hiding_any_leaves_reads_as_the_tree_without_them() {
        let mut cases = 0;
        for n in 1..=5 {
            for tree in trees(n, 3) {
                let stored = leaves(tree.clone(), || NIL).unwrap();
                for (_, leaf) in &stored {
                    assert_eq!(Leaf::of(NESTED, leaf.nested_position()), *leaf);
                }
                assert_eq!(items(stored.clone()), tree);
                let shapes: Vec<Leaf> = stored.iter().map(|(_, l)| *l).collect();
                for mask in 1u32..(1 << n) {
                    let gone = |x: u32| mask & (1 << x) != 0;
                    let want = prune(&tree, &gone);
                    let hidden = stored.iter().map(|(x, l)| ((!gone(*x)).then_some(*x), *l));
                    assert_eq!(kept_items(hidden), want, "{tree:?} hiding {mask:b}");
                    let mut survivors = shapes.clone();
                    for (k, repaired) in repair(&shapes, |k| gone(stored[k].0)) {
                        survivors[k] = repaired;
                    }
                    let repaired = stored
                        .iter()
                        .zip(survivors)
                        .filter(|((x, _), _)| !gone(*x))
                        .map(|((x, _), leaf)| (*x, leaf));
                    assert_eq!(items(repaired), want, "{tree:?} deleting {mask:b}");
                    cases += 1;
                }
            }
        }
        assert!(cases > 10_000, "{cases}");
    }

    #[test]
    fn the_last_deleted_leaf_alone_does_not_repair_a_run() {
        // ((1) (2 3 4)) without 2 and 3 is ((1) (4)), not ((1 4)).
        let tree = vec![l(vec![v(1)]), l(vec![v(2), v(3), v(4)])];
        let stored = leaves(tree, || NIL).unwrap();
        let shapes: Vec<Leaf> = stored.iter().map(|(_, l)| *l).collect();
        let changes = repair(&shapes, |k| k == 1 || k == 2);
        assert_eq!(changes.len(), 1);
        assert_eq!((changes[0].0, changes[0].1.shape()), (3, (1, 1)));
    }

    #[test]
    fn any_bits_decode_without_panicking() {
        for depth_a in 0..=MAX_DEPTH {
            for opens_a in 0..=0xf {
                for depth_b in 0..=MAX_DEPTH {
                    for opens_b in 0..=0xf {
                        let leaf = |pos, depth, opens| Leaf { pos, depth, opens };
                        let got = items([
                            (1, leaf(0, depth_a, opens_a)),
                            (2, leaf(1, depth_b, opens_b)),
                        ]);
                        fn count(items: &[Item<u32>]) -> usize {
                            items
                                .iter()
                                .map(|i| match i {
                                    Item::Value(_) => 1,
                                    Item::List(inner) => count(inner),
                                })
                                .sum()
                        }
                        assert_eq!(count(&got), 2);
                    }
                }
            }
        }
    }
}
