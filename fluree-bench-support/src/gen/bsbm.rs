//! BSBM-shape data generator.
//!
//! Produces a deterministic graph with the four entities at the heart
//! of the [Berlin SPARQL Benchmark](http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/):
//! `Vendor`, `Product`, `Person`, `Review`. The shape supports BSBM
//! query patterns Q3 (multi-hop filter), Q5 (multi-join with range
//! filter), and Q9 (group + count + having) — the three the
//! `query_hot_bsbm` bench exercises.
//!
//! ## Why generate, not vendor
//!
//! At BSBM-1K scale the canonical Turtle file is ~5 MB. Vendoring a
//! binary-ish blob in git would inflate the repo and the
//! download/clone footprint of every contributor. Generating on the
//! fly:
//!
//! - Keeps the repo small.
//! - Lets the bench scale across `BenchScale` tiers without
//!   maintaining multiple vendored sizes.
//! - Avoids a build-time dependency on
//!   [`bsbmtools`](https://github.com/wbsg-uni-mannheim/bsbmtools).
//! - Stays deterministic (the `Vec`/`String` outputs are byte-identical
//!   across runs given the same `n_products`).
//!
//! For multi-million-triple scales (Large+ in `BenchScale`) we'd
//! still want the canonical generator's distributions; that's a
//! follow-up if a nightly run discovers our shape diverges enough to
//! matter.
//!
//! ## Data model (subset)
//!
//! ```text
//! ex:vendor-N    a bsbm:Vendor   ; bsbm:label ; bsbm:country .
//! ex:product-N   a bsbm:Product  ; bsbm:label ; bsbm:vendor ;
//!                                  bsbm:productType ; bsbm:price .
//! ex:person-N    a bsbm:Person   ; bsbm:name .
//! ex:review-N    a bsbm:Review   ; bsbm:reviewFor ; bsbm:reviewer ;
//!                                  bsbm:rating ; bsbm:text .
//! ```
//!
//! Counts scale off `n_products`:
//!
//! - `n_vendors  = max(1, n_products / 50)`
//! - `n_persons  = max(1, n_products / 10)`
//! - `n_reviews  = n_products * 3`     (every product has ~3 reviews)
//! - `n_types    = 5`                  (fixed; products cycle through them)

const BSBM_NS: &str = "http://example.org/bsbm/";
const PRODUCT_TYPES: &[&str] = &["Electronics", "Books", "Clothing", "Sports", "HomeGoods"];
const COUNTRIES: &[&str] = &["US", "DE", "GB", "JP", "BR"];

#[derive(Debug, Clone)]
pub struct Vendor {
    pub id: String,
    pub label: String,
    pub country: &'static str,
}

#[derive(Debug, Clone)]
pub struct Product {
    pub id: String,
    pub label: String,
    pub product_type: &'static str,
    pub vendor_id: String,
    pub price_cents: u32,
}

#[derive(Debug, Clone)]
pub struct Person {
    pub id: String,
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct Review {
    pub id: String,
    pub product_id: String,
    pub reviewer_id: String,
    pub rating: u32,
    pub text: String,
}

#[derive(Debug, Clone)]
pub struct BsbmData {
    pub vendors: Vec<Vendor>,
    pub products: Vec<Product>,
    pub persons: Vec<Person>,
    pub reviews: Vec<Review>,
}

impl BsbmData {
    /// Estimated triple count of the Turtle serialization. Used by
    /// benches for `Throughput::Elements`.
    pub fn estimated_triples(&self) -> u64 {
        // Vendor: 3 (a, label, country)
        // Product: 5 (a, label, productType, vendor, price)
        // Person: 2 (a, name)
        // Review: 5 (a, reviewFor, reviewer, rating, text)
        (self.vendors.len() * 3
            + self.products.len() * 5
            + self.persons.len() * 2
            + self.reviews.len() * 5) as u64
    }
}

/// Generate a deterministic BSBM-shape dataset with the given product
/// count. Other entity counts derive from `n_products`.
pub fn generate_dataset(n_products: usize) -> BsbmData {
    assert!(n_products >= 1, "n_products must be >= 1");
    let n_vendors = std::cmp::max(1, n_products / 50);
    let n_persons = std::cmp::max(1, n_products / 10);
    let n_reviews = n_products * 3;

    let vendors: Vec<Vendor> = (0..n_vendors)
        .map(|i| Vendor {
            id: format!("ex:vendor-{i:06}"),
            label: format!("Vendor {i:06}"),
            country: COUNTRIES[i % COUNTRIES.len()],
        })
        .collect();

    let products: Vec<Product> = (0..n_products)
        .map(|i| {
            // Deterministic price in [10.00, 510.00] cents (i.e., $0.10
            // to $5.10) — small range so rangescan benches see meaningful
            // selectivity across the product set.
            let price_cents = 1_000 + ((i * 37) % 50_000) as u32;
            Product {
                id: format!("ex:product-{i:06}"),
                label: format!("Product {i:06}"),
                product_type: PRODUCT_TYPES[i % PRODUCT_TYPES.len()],
                vendor_id: vendors[i % vendors.len()].id.clone(),
                price_cents,
            }
        })
        .collect();

    let persons: Vec<Person> = (0..n_persons)
        .map(|i| Person {
            id: format!("ex:person-{i:06}"),
            name: format!("Person {i:06}"),
        })
        .collect();

    // Reviews: ~3 per product, distributed deterministically over
    // persons. Ratings cycle 1..=5 so HAVING / ORDER BY scenarios have
    // genuine variation.
    let reviews: Vec<Review> = (0..n_reviews)
        .map(|i| {
            let prod_idx = i / 3;
            let reviewer_idx = (i * 7) % persons.len();
            let rating = 1 + (i % 5) as u32;
            Review {
                id: format!("ex:review-{i:06}"),
                product_id: products[prod_idx].id.clone(),
                reviewer_id: persons[reviewer_idx].id.clone(),
                rating,
                text: format!("Review text {i:06} — useful and informative."),
            }
        })
        .collect();

    BsbmData {
        vendors,
        products,
        persons,
        reviews,
    }
}

/// Render `BsbmData` as a Turtle document with the standard
/// `ex:` and `bsbm:` prefixes plus `xsd:`.
pub fn bsbm_data_to_turtle(data: &BsbmData) -> String {
    // Conservative cap: ~120 bytes per entity on average.
    let cap =
        (data.vendors.len() + data.products.len() + data.persons.len() + data.reviews.len()) * 150;
    let mut buf = String::with_capacity(cap);

    buf.push_str("@prefix ex: <http://example.org/ns/> .\n");
    buf.push_str(&format!("@prefix bsbm: <{BSBM_NS}> .\n"));
    buf.push_str("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\n");

    for v in &data.vendors {
        buf.push_str(&v.id);
        buf.push_str(" a bsbm:Vendor ;\n");
        buf.push_str(&format!("    bsbm:label \"{}\" ;\n", v.label));
        buf.push_str(&format!("    bsbm:country \"{}\" .\n\n", v.country));
    }

    for p in &data.products {
        buf.push_str(&p.id);
        buf.push_str(" a bsbm:Product ;\n");
        buf.push_str(&format!("    bsbm:label \"{}\" ;\n", p.label));
        buf.push_str(&format!("    bsbm:productType \"{}\" ;\n", p.product_type));
        buf.push_str(&format!("    bsbm:vendor {} ;\n", p.vendor_id));
        buf.push_str(&format!(
            "    bsbm:price \"{}\"^^xsd:integer .\n\n",
            p.price_cents
        ));
    }

    for p in &data.persons {
        buf.push_str(&p.id);
        buf.push_str(" a bsbm:Person ;\n");
        buf.push_str(&format!("    bsbm:name \"{}\" .\n\n", p.name));
    }

    for r in &data.reviews {
        buf.push_str(&r.id);
        buf.push_str(" a bsbm:Review ;\n");
        buf.push_str(&format!("    bsbm:reviewFor {} ;\n", r.product_id));
        buf.push_str(&format!("    bsbm:reviewer {} ;\n", r.reviewer_id));
        buf.push_str(&format!(
            "    bsbm:rating \"{}\"^^xsd:integer ;\n",
            r.rating
        ));
        // Keep review text as a plain string literal — no embedded
        // double-quotes since the templates above are quote-safe.
        buf.push_str(&format!("    bsbm:text \"{}\" .\n\n", r.text));
    }

    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_across_runs() {
        let a = generate_dataset(100);
        let b = generate_dataset(100);
        assert_eq!(a.products.len(), b.products.len());
        assert_eq!(a.vendors.len(), b.vendors.len());
        assert_eq!(a.persons.len(), b.persons.len());
        assert_eq!(a.reviews.len(), b.reviews.len());
        assert_eq!(a.products[0].price_cents, b.products[0].price_cents);
        assert_eq!(a.reviews[0].rating, b.reviews[0].rating);
    }

    #[test]
    fn count_ratios() {
        let d = generate_dataset(500);
        assert_eq!(d.products.len(), 500);
        assert_eq!(d.vendors.len(), 10); // 500 / 50
        assert_eq!(d.persons.len(), 50); // 500 / 10
        assert_eq!(d.reviews.len(), 1500); // 500 * 3
    }

    #[test]
    fn small_dataset_has_at_least_one_of_each() {
        let d = generate_dataset(1);
        assert_eq!(d.products.len(), 1);
        assert!(!d.vendors.is_empty());
        assert!(!d.persons.is_empty());
        assert!(!d.reviews.is_empty());
    }

    #[test]
    fn product_types_distribute() {
        let d = generate_dataset(50);
        let types: std::collections::HashSet<_> =
            d.products.iter().map(|p| p.product_type).collect();
        // With 50 products and 5 types, every type should appear.
        assert_eq!(types.len(), PRODUCT_TYPES.len());
    }

    #[test]
    fn ratings_distribute() {
        let d = generate_dataset(20);
        let ratings: std::collections::HashSet<_> = d.reviews.iter().map(|r| r.rating).collect();
        // 60 reviews × cycle 1..=5 ⇒ all five ratings present.
        assert_eq!(ratings.len(), 5);
    }

    #[test]
    fn turtle_has_prefixes() {
        let d = generate_dataset(5);
        let s = bsbm_data_to_turtle(&d);
        assert!(s.contains("@prefix ex:"));
        assert!(s.contains("@prefix bsbm:"));
        assert!(s.contains("@prefix xsd:"));
        assert!(s.contains("a bsbm:Product"));
        assert!(s.contains("a bsbm:Review"));
    }

    #[test]
    fn estimated_triples_reasonable() {
        let d = generate_dataset(100);
        // 100 * 5 (products) + 2 (vendors) * 3 + 10 (persons) * 2 + 300 (reviews) * 5
        // = 500 + 6 + 20 + 1500 = 2026
        assert_eq!(d.estimated_triples(), 2026);
    }
}
