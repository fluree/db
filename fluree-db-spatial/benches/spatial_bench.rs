//! Spatial index benchmarks.
//!
//! Measures:
//! - Build time (geometry → S2 covering → index construction)
//! - Query latency (within/intersects/radius operations)
//! - Covering quality (false positive rate)

use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use fluree_db_spatial::config::SpatialCreateConfig;
use fluree_db_spatial::{SpatialIndexBuilder, SpatialIndexSnapshot};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

// ============================================================================
// Test Data Generation
// ============================================================================

/// Generate a simple polygon at a given center with approximate size in degrees.
fn generate_polygon(center_lat: f64, center_lng: f64, size_deg: f64) -> String {
    let half = size_deg / 2.0;
    format!(
        "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))",
        center_lng - half,
        center_lat - half,
        center_lng + half,
        center_lat - half,
        center_lng + half,
        center_lat + half,
        center_lng - half,
        center_lat + half,
        center_lng - half,
        center_lat - half,
    )
}

/// Generate a more complex polygon (hexagon) at a given center.
fn generate_hexagon(center_lat: f64, center_lng: f64, size_deg: f64) -> String {
    let r = size_deg / 2.0;
    let mut coords = Vec::with_capacity(7);
    for i in 0..6 {
        let angle = (i as f64) * std::f64::consts::PI / 3.0;
        let x = center_lng + r * angle.cos();
        let y = center_lat + r * angle.sin();
        coords.push(format!("{x} {y}"));
    }
    coords.push(coords[0].clone()); // Close the ring
    format!("POLYGON(({}))", coords.join(", "))
}

/// Generate test geometries spread across a region.
fn generate_test_geometries(
    count: usize,
    center_lat: f64,
    center_lng: f64,
    spread_deg: f64,
) -> Vec<(u64, String)> {
    let mut geometries = Vec::with_capacity(count);
    let sqrt_count = (count as f64).sqrt().ceil() as usize;
    let step = spread_deg / sqrt_count as f64;

    for i in 0..count {
        let row = i / sqrt_count;
        let col = i % sqrt_count;
        let lat = center_lat - spread_deg / 2.0 + row as f64 * step;
        let lng = center_lng - spread_deg / 2.0 + col as f64 * step;

        // Alternate between simple squares and hexagons
        let wkt = if i % 2 == 0 {
            generate_polygon(lat, lng, 0.01)
        } else {
            generate_hexagon(lat, lng, 0.01)
        };

        geometries.push((i as u64 + 1000, wkt));
    }

    geometries
}

// ============================================================================
// Build Benchmarks
// ============================================================================

fn bench_build_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("build_index");

    for count in [100, 1000, 10000] {
        // Pre-generate geometries
        let geometries = generate_test_geometries(count, 48.8566, 2.3522, 1.0);

        group.throughput(Throughput::Elements(count as u64));
        group.bench_with_input(
            BenchmarkId::new("add_geometry", count),
            &geometries,
            |b, geoms| {
                b.iter(|| {
                    let config = SpatialCreateConfig::new("geo:bench", "ledger:bench", "geo:asWKT");
                    let mut builder = SpatialIndexBuilder::new(config);

                    for (subject_id, wkt) in geoms {
                        let _ = builder.add_geometry(*subject_id, wkt, 100, true);
                    }

                    black_box(builder.stats().geometries_added)
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("full_build", count),
            &geometries,
            |b, geoms| {
                b.iter(|| {
                    let config = SpatialCreateConfig::new("geo:bench", "ledger:bench", "geo:asWKT");
                    let mut builder = SpatialIndexBuilder::new(config);

                    for (subject_id, wkt) in geoms {
                        let _ = builder.add_geometry(*subject_id, wkt, 100, true);
                    }

                    let result = builder.build().unwrap();
                    black_box(result.stats.cell_entries)
                });
            },
        );
    }

    group.finish();
}

fn bench_covering_generation(c: &mut Criterion) {
    use fluree_db_spatial::config::S2CoveringConfig;

    let mut group = c.benchmark_group("covering_generation");

    // Test different geometry sizes
    let sizes = [
        ("small_0.01deg", 0.01),
        ("medium_0.1deg", 0.1),
        ("large_1deg", 1.0),
    ];

    for (name, size) in sizes {
        let wkt = generate_polygon(48.8566, 2.3522, size);
        let geom = fluree_db_spatial::geometry::parse_wkt(&wkt).unwrap();
        let config = S2CoveringConfig::default();

        group.bench_with_input(
            BenchmarkId::new("polygon", name),
            &(&geom, &config),
            |b, (geom, config)| {
                b.iter(|| {
                    let cells =
                        fluree_db_spatial::covering::covering_for_geometry(geom, config).unwrap();
                    black_box(cells.len())
                });
            },
        );
    }

    // Test circle covering (for radius queries)
    for radius in [1000.0, 10000.0, 100_000.0] {
        let config = S2CoveringConfig::default();

        group.bench_with_input(
            BenchmarkId::new("circle_m", radius as u64),
            &(&config, radius),
            |b, (config, radius)| {
                b.iter(|| {
                    let cells = fluree_db_spatial::covering::covering_for_circle(
                        48.8566, 2.3522, *radius, config,
                    )
                    .unwrap();
                    black_box(cells.len())
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Query Benchmarks
// ============================================================================

fn bench_query_operations(c: &mut Criterion) {
    // Build an index with test data
    let geometries = generate_test_geometries(1000, 48.8566, 2.3522, 1.0);
    let config = SpatialCreateConfig::new("geo:bench", "ledger:bench", "geo:asWKT");
    let mut builder = SpatialIndexBuilder::new(config);

    for (subject_id, wkt) in &geometries {
        let _ = builder.add_geometry(*subject_id, wkt, 100, true);
    }

    let result = builder.build().unwrap();

    // Write to in-memory CAS
    let cas: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
    let cas_write = cas.clone();

    let mut counter = 0u32;
    let write_result = result
        .write_to_cas(|bytes| {
            counter += 1;
            let hash = format!("sha256:{counter:08x}");
            cas_write
                .write()
                .unwrap()
                .insert(hash.clone(), bytes.to_vec());
            Ok(hash)
        })
        .unwrap();

    // Load snapshot
    let cas_read = cas.clone();
    let snapshot = SpatialIndexSnapshot::load_from_cas(write_result.root.clone(), move |hash| {
        cas_read.read().unwrap().get(hash).cloned().ok_or_else(|| {
            fluree_db_spatial::error::SpatialError::FormatError(format!("hash not found: {hash}"))
        })
    })
    .unwrap();

    // Print selectivity stats before benchmarks
    println!("\n=== Query Selectivity Statistics (1000 geometries) ===\n");

    // Radius queries
    for radius in [1000.0, 10000.0, 50000.0] {
        let (results, stats) = snapshot
            .query_radius_with_stats(48.8566, 2.3522, radius, 100, None)
            .unwrap();
        println!("radius {}m:", radius as u64);
        println!(
            "  covering_cells: {}, ranges: {}",
            stats.covering_cells, stats.ranges_scanned
        );
        println!(
            "  snapshot_entries: {}, novelty: {}",
            stats.snapshot_entries, stats.novelty_entries
        );
        println!(
            "  after_replay: {}, passed_bbox: {}, exact_checks: {}",
            stats.after_replay, stats.passed_bbox, stats.exact_checks
        );
        println!(
            "  results: {}, selectivity: {:.2}%, bbox_eff: {:.2}%",
            results.len(),
            stats.selectivity() * 100.0,
            stats.bbox_efficiency() * 100.0
        );
        println!();
    }

    // Within/intersects queries
    let query_sizes = [("small", 0.1), ("medium", 0.5), ("large", 1.0)];

    for (name, size) in &query_sizes {
        let query_wkt = generate_polygon(48.8566, 2.3522, *size);
        let query_geom = fluree_db_spatial::geometry::parse_wkt(&query_wkt).unwrap();

        let (results, stats) = snapshot
            .query_within_with_stats(&query_geom, 100, None)
            .unwrap();
        println!("within {name} ({size}°):");
        println!(
            "  covering_cells: {}, ranges: {}",
            stats.covering_cells, stats.ranges_scanned
        );
        println!(
            "  snapshot_entries: {}, after_replay: {}, after_dedup: {}",
            stats.snapshot_entries, stats.after_replay, stats.after_dedup
        );
        println!(
            "  passed_bbox: {}, exact_checks: {}, results: {}",
            stats.passed_bbox,
            stats.exact_checks,
            results.len()
        );
        println!(
            "  selectivity: {:.2}%, exact_check_eff: {:.2}%",
            stats.selectivity() * 100.0,
            stats.exact_check_efficiency() * 100.0
        );
        println!();

        let (results, stats) = snapshot
            .query_intersects_with_stats(&query_geom, 100, None)
            .unwrap();
        println!("intersects {name} ({size}°):");
        println!(
            "  covering_cells: {}, ranges: {}",
            stats.covering_cells, stats.ranges_scanned
        );
        println!(
            "  snapshot_entries: {}, after_replay: {}, after_dedup: {}",
            stats.snapshot_entries, stats.after_replay, stats.after_dedup
        );
        println!(
            "  passed_bbox: {}, exact_checks: {}, results: {}",
            stats.passed_bbox,
            stats.exact_checks,
            results.len()
        );
        println!(
            "  selectivity: {:.2}%, exact_check_eff: {:.2}%",
            stats.selectivity() * 100.0,
            stats.exact_check_efficiency() * 100.0
        );
        println!();
    }

    let mut group = c.benchmark_group("query_operations");

    // Query: radius search
    for radius in [1000.0, 10000.0, 50000.0] {
        group.bench_with_input(
            BenchmarkId::new("radius_m", radius as u64),
            &(&snapshot, radius),
            |b, (snapshot, radius)| {
                b.iter(|| {
                    let results = snapshot
                        .query_radius(48.8566, 2.3522, *radius, 100, None)
                        .unwrap();
                    black_box(results.len())
                });
            },
        );
    }

    // Query: within polygon
    for (name, size) in query_sizes {
        let query_wkt = generate_polygon(48.8566, 2.3522, size);
        let query_geom = fluree_db_spatial::geometry::parse_wkt(&query_wkt).unwrap();

        group.bench_with_input(
            BenchmarkId::new("within", name),
            &(&snapshot, &query_geom),
            |b, (snapshot, query_geom)| {
                b.iter(|| {
                    let results = snapshot.query_within(query_geom, 100, None).unwrap();
                    black_box(results.len())
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("intersects", name),
            &(&snapshot, &query_geom),
            |b, (snapshot, query_geom)| {
                b.iter(|| {
                    let results = snapshot.query_intersects(query_geom, 100, None).unwrap();
                    black_box(results.len())
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Covering Quality Measurement
// ============================================================================

fn bench_covering_quality(c: &mut Criterion) {
    use fluree_db_spatial::config::S2CoveringConfig;

    let mut group = c.benchmark_group("covering_quality");

    // This benchmark measures the false positive rate of S2 coverings
    // by comparing the covering's bounding area to the actual geometry area

    let test_cases = [
        ("square_0.1deg", generate_polygon(48.8566, 2.3522, 0.1)),
        ("square_1deg", generate_polygon(48.8566, 2.3522, 1.0)),
        ("hexagon_0.1deg", generate_hexagon(48.8566, 2.3522, 0.1)),
        ("hexagon_1deg", generate_hexagon(48.8566, 2.3522, 1.0)),
    ];

    // First, print covering statistics (not timed)
    println!("\n=== Covering Quality Statistics ===");
    let config = S2CoveringConfig::default();
    for (name, wkt) in &test_cases {
        let geom = fluree_db_spatial::geometry::parse_wkt(wkt).unwrap();
        let cells = fluree_db_spatial::covering::covering_for_geometry(&geom, &config).unwrap();
        println!("{}: {} cells", name, cells.len());
    }
    println!();

    for (name, wkt) in test_cases {
        let geom = fluree_db_spatial::geometry::parse_wkt(&wkt).unwrap();
        let config = S2CoveringConfig::default();

        group.bench_with_input(
            BenchmarkId::new("covering_cells", name),
            &(&geom, &config),
            |b, (geom, config)| {
                b.iter(|| {
                    let cells =
                        fluree_db_spatial::covering::covering_for_geometry(geom, config).unwrap();
                    // Measure: number of cells (lower is better for selectivity)
                    black_box(cells.len())
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Novelty Overlay Benchmarks
// ============================================================================

fn bench_novelty_overlay(c: &mut Criterion) {
    use fluree_db_spatial::CellEntry;

    // Build base index with 1000 geometries
    let geometries = generate_test_geometries(1000, 48.8566, 2.3522, 1.0);
    let config = SpatialCreateConfig::new("geo:bench", "ledger:bench", "geo:asWKT");
    let mut builder = SpatialIndexBuilder::new(config.clone());

    for (subject_id, wkt) in &geometries {
        let _ = builder.add_geometry(*subject_id, wkt, 100, true);
    }

    let result = builder.build().unwrap();

    // Write to CAS
    let cas: Arc<RwLock<HashMap<String, Vec<u8>>>> = Arc::new(RwLock::new(HashMap::new()));
    let cas_write = cas.clone();

    let mut counter = 0u32;
    let write_result = result
        .write_to_cas(|bytes| {
            counter += 1;
            let hash = format!("sha256:{counter:08x}");
            cas_write
                .write()
                .unwrap()
                .insert(hash.clone(), bytes.to_vec());
            Ok(hash)
        })
        .unwrap();

    // Load base snapshot
    let cas_read = cas.clone();
    let mut snapshot =
        SpatialIndexSnapshot::load_from_cas(write_result.root.clone(), move |hash| {
            cas_read.read().unwrap().get(hash).cloned().ok_or_else(|| {
                fluree_db_spatial::error::SpatialError::FormatError(format!(
                    "hash not found: {hash}"
                ))
            })
        })
        .unwrap();

    // Generate novelty entries (new geometries at t=101)
    fn generate_novelty(count: usize, config: &SpatialCreateConfig) -> Vec<CellEntry> {
        let mut novelty_builder = SpatialIndexBuilder::new(config.clone());
        for i in 0..count {
            let lat = 48.8566 + (i as f64 * 0.001);
            let lng = 2.3522 + (i as f64 * 0.001);
            let wkt = format!(
                "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))",
                lng - 0.005,
                lat - 0.005,
                lng + 0.005,
                lat - 0.005,
                lng + 0.005,
                lat + 0.005,
                lng - 0.005,
                lat + 0.005,
                lng - 0.005,
                lat - 0.005,
            );
            let _ = novelty_builder.add_geometry(10000 + i as u64, &wkt, 101, true);
        }
        let (entries, _, _) = novelty_builder.finalize();
        entries
    }

    let query_wkt = generate_polygon(48.8566, 2.3522, 0.5);
    let query_geom = fluree_db_spatial::geometry::parse_wkt(&query_wkt).unwrap();

    println!("\n=== Novelty Overlay Benchmark ===\n");

    let mut group = c.benchmark_group("novelty_overlay");

    // 0% novelty (baseline)
    group.bench_function("0%_novelty", |b| {
        b.iter(|| {
            let results = snapshot.query_within(&query_geom, 101, None).unwrap();
            black_box(results.len())
        });
    });

    // 1% novelty (10 entries)
    let novelty_1pct = generate_novelty(10, &config);
    snapshot.set_novelty(novelty_1pct, 1);
    group.bench_function("1%_novelty", |b| {
        b.iter(|| {
            let results = snapshot.query_within(&query_geom, 101, None).unwrap();
            black_box(results.len())
        });
    });

    // 10% novelty (100 entries)
    let novelty_10pct = generate_novelty(100, &config);
    snapshot.set_novelty(novelty_10pct, 2);
    group.bench_function("10%_novelty", |b| {
        b.iter(|| {
            let results = snapshot.query_within(&query_geom, 101, None).unwrap();
            black_box(results.len())
        });
    });

    group.finish();
}

// ============================================================================
// Edge Case Tests (Antimeridian, Poles, Large Polygons)
// ============================================================================

fn bench_edge_cases(c: &mut Criterion) {
    use fluree_db_spatial::config::S2CoveringConfig;

    println!("\n=== Edge Case Analysis ===\n");

    // Test cases for edge conditions
    let edge_cases = [
        // Antimeridian crossing polygon
        (
            "antimeridian",
            "POLYGON((170 0, -170 0, -170 10, 170 10, 170 0))",
        ),
        // Near-pole polygon (Arctic)
        (
            "arctic_85N",
            "POLYGON((-180 85, 0 85, 180 85, 180 89, 0 89, -180 89, -180 85))",
        ),
        // Near-pole polygon (Antarctic)
        (
            "antarctic_85S",
            "POLYGON((-180 -89, 0 -89, 180 -89, 180 -85, 0 -85, -180 -85, -180 -89))",
        ),
        // Very large polygon (continent-sized)
        ("large_30deg", "POLYGON((0 0, 30 0, 30 30, 0 30, 0 0))"),
        // Thin polygon (long and narrow)
        ("thin_1x10deg", "POLYGON((0 0, 10 0, 10 0.1, 0 0.1, 0 0))"),
    ];

    let config = S2CoveringConfig::default();

    for (name, wkt) in &edge_cases {
        match fluree_db_spatial::geometry::parse_wkt(wkt) {
            Ok(geom) => match fluree_db_spatial::covering::covering_for_geometry(&geom, &config) {
                Ok(cells) => {
                    let ranges = fluree_db_spatial::covering::cells_to_ranges(&cells);
                    println!("{}: {} cells, {} ranges", name, cells.len(), ranges.len());
                }
                Err(e) => {
                    println!("{name}: covering failed: {e}");
                }
            },
            Err(e) => {
                println!("{name}: parse failed: {e}");
            }
        }
    }
    println!();

    // Benchmark edge cases that successfully parse
    let mut group = c.benchmark_group("edge_cases");

    // Antimeridian query
    let antimeridian_wkt = "POLYGON((170 0, -170 0, -170 10, 170 10, 170 0))";
    if let Ok(geom) = fluree_db_spatial::geometry::parse_wkt(antimeridian_wkt) {
        group.bench_function("covering_antimeridian", |b| {
            b.iter(|| {
                let cells =
                    fluree_db_spatial::covering::covering_for_geometry(&geom, &config).unwrap();
                black_box(cells.len())
            });
        });
    }

    // Large polygon covering
    let large_wkt = "POLYGON((0 0, 30 0, 30 30, 0 30, 0 0))";
    if let Ok(geom) = fluree_db_spatial::geometry::parse_wkt(large_wkt) {
        group.bench_function("covering_large_30deg", |b| {
            b.iter(|| {
                let cells =
                    fluree_db_spatial::covering::covering_for_geometry(&geom, &config).unwrap();
                black_box(cells.len())
            });
        });
    }

    // Thin polygon covering
    let thin_wkt = "POLYGON((0 0, 10 0, 10 0.1, 0 0.1, 0 0))";
    if let Ok(geom) = fluree_db_spatial::geometry::parse_wkt(thin_wkt) {
        group.bench_function("covering_thin", |b| {
            b.iter(|| {
                let cells =
                    fluree_db_spatial::covering::covering_for_geometry(&geom, &config).unwrap();
                black_box(cells.len())
            });
        });
    }

    group.finish();
}

// ============================================================================
// Covering Configuration Analysis
// ============================================================================

fn bench_covering_config_analysis(c: &mut Criterion) {
    use fluree_db_spatial::config::S2CoveringConfig;

    println!("\n=== Covering Configuration Analysis ===");
    println!("Testing how max_cells affects covering tightness\n");

    let test_geoms = [
        ("square_0.1deg", generate_polygon(48.8566, 2.3522, 0.1)),
        ("square_1deg", generate_polygon(48.8566, 2.3522, 1.0)),
    ];

    let max_cells_configs = [4, 8, 16, 32];

    for (name, wkt) in &test_geoms {
        let geom = fluree_db_spatial::geometry::parse_wkt(wkt).unwrap();
        println!("{name}:");

        for &max_cells in &max_cells_configs {
            let config = S2CoveringConfig {
                min_level: 4,
                max_level: 16,
                max_cells,
            };
            let cells = fluree_db_spatial::covering::covering_for_geometry(&geom, &config).unwrap();
            println!(
                "  max_cells={:2}: {} cells generated",
                max_cells,
                cells.len()
            );
        }
        println!();
    }

    // Benchmark with different max_cells settings
    let mut group = c.benchmark_group("covering_config");

    let geom =
        fluree_db_spatial::geometry::parse_wkt(&generate_polygon(48.8566, 2.3522, 0.5)).unwrap();

    for &max_cells in &max_cells_configs {
        let config = S2CoveringConfig {
            min_level: 4,
            max_level: 16,
            max_cells,
        };

        group.bench_with_input(
            BenchmarkId::new("max_cells", max_cells),
            &(&geom, config),
            |b, (geom, config)| {
                b.iter(|| {
                    let cells =
                        fluree_db_spatial::covering::covering_for_geometry(geom, config).unwrap();
                    black_box(cells.len())
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Main
// ============================================================================

criterion_group!(
    benches,
    bench_build_index,
    bench_covering_generation,
    bench_query_operations,
    bench_covering_quality,
    bench_covering_config_analysis,
    bench_novelty_overlay,
    bench_edge_cases,
);

criterion_main!(benches);
