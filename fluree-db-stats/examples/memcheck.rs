use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

static LIVE: AtomicUsize = AtomicUsize::new(0);

struct Counting;
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        LIVE.fetch_add(l.size(), Ordering::Relaxed);
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new: usize) -> *mut u8 {
        LIVE.fetch_add(new, Ordering::Relaxed);
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        unsafe { System.realloc(p, l, new) }
    }
}

#[global_allocator]
static A: Counting = Counting;

use fluree_db_stats::grouped::GroupedProfile;
use fluree_db_stats::profile::{ColumnProfile, ProfileConfig, ProfileValue};

fn mib(b: usize) -> f64 {
    b as f64 / 1024.0 / 1024.0
}

fn scenario(name: &str, groups: usize, per_group: usize) {
    let before = LIVE.load(Ordering::Relaxed);
    let mut g = GroupedProfile::new(ProfileConfig::default(), usize::MAX);
    for i in 0..groups {
        let key = format!("g-{i}");
        for j in 0..per_group {
            g.observe(&key, ProfileValue::Float((i * per_group + j) as f64));
        }
    }
    let after = LIVE.load(Ordering::Relaxed);
    let used = after - before;
    println!(
        "{name:<28} {groups:>7} groups x {per_group:<4} vals = {:>8.1} MiB  ({:>7} B/group)",
        mib(used),
        used / groups
    );
    drop(g);
}

fn main() {
    println!(
        "size_of::<ColumnProfile>() = {}",
        size_of::<ColumnProfile>()
    );
    println!(
        "DEFAULT_MAX_GROUPS         = {}",
        fluree_db_stats::grouped::DEFAULT_MAX_GROUPS
    );
    println!();
    scenario("many tiny groups", 100_000, 1);
    scenario("fewer large groups", 10_000, 600);
    scenario("at the new default", 10_000, 1);
}
