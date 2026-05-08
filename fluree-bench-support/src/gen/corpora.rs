//! Text corpora for full-text and tokenization benchmarks.
//!
//! Lifted from `fulltext_query.rs`. The constants below are byte-identical
//! to that file's; benches retrofitted to use this module produce the same
//! result they did pre-chassis.

use rand::Rng;

/// Paragraph-length templates with domain vocabulary. Each template is
/// 30–60 words. Sixteen templates total; pick uniformly with
/// `rng.gen_range(0..PARAGRAPH_TEMPLATES.len())`.
pub const PARAGRAPH_TEMPLATES: &[&str] = &[
    "The rapid advancement of distributed database systems has fundamentally \
     transformed how organizations manage and query large-scale data. Modern \
     approaches leverage columnar storage, immutable ledgers, and semantic \
     graph models to achieve both performance and correctness guarantees \
     that were previously unattainable.",
    "Machine learning algorithms continue to reshape scientific research \
     across multiple disciplines. From protein folding predictions to climate \
     modeling, neural networks provide powerful tools for pattern recognition \
     in complex datasets that defy traditional analytical methods.",
    "Sustainable energy infrastructure requires careful integration of \
     renewable sources with existing power grids. Battery storage technology, \
     smart grid management, and demand response systems form the backbone \
     of modern energy transition strategies in urban environments.",
    "The evolution of programming languages reflects changing priorities \
     in software engineering. Memory safety, concurrency primitives, and \
     type system expressiveness have become critical design considerations \
     as systems grow more complex and security threats intensify.",
    "Quantum computing research has reached an inflection point where \
     practical applications begin to emerge alongside theoretical advances. \
     Error correction techniques and hybrid classical-quantum algorithms \
     show promise for optimization problems in logistics and cryptography.",
    "Urban planning in the twenty-first century must balance population \
     growth with environmental sustainability. Mixed-use development, \
     public transit investment, and green infrastructure provide frameworks \
     for creating resilient cities that serve diverse communities.",
    "Genomic medicine is revolutionizing healthcare through personalized \
     treatment protocols based on individual genetic profiles. Advances in \
     sequencing technology and bioinformatics tools enable clinicians to \
     identify disease markers and therapeutic targets with unprecedented precision.",
    "The intersection of artificial intelligence and natural language \
     processing has produced remarkable advances in text understanding. \
     Large language models demonstrate emergent capabilities in reasoning, \
     summarization, and knowledge synthesis across diverse domains.",
    "Ocean conservation efforts increasingly rely on satellite monitoring \
     and underwater sensor networks to track marine ecosystem health. \
     Real-time data collection enables rapid response to pollution events \
     and supports evidence-based fishery management policies.",
    "Cybersecurity frameworks must evolve continuously to address emerging \
     threat vectors in cloud-native architectures. Zero trust principles, \
     supply chain verification, and automated incident response form the \
     foundation of modern defensive security postures.",
    "Archaeological discoveries continue to reshape our understanding of \
     ancient civilizations and their technological achievements. Advanced \
     imaging techniques and isotope analysis reveal migration patterns, \
     trade networks, and cultural exchanges spanning millennia.",
    "The global semiconductor industry faces unprecedented demand driven \
     by artificial intelligence workloads and Internet of Things devices. \
     Advanced fabrication processes at nanometer scales push the boundaries \
     of materials science and precision manufacturing.",
    "Blockchain technology extends beyond cryptocurrency to enable verifiable \
     credentials, supply chain transparency, and decentralized governance. \
     Immutable ledger architectures provide audit trails and trust frameworks \
     for multi-party transactions without centralized intermediaries.",
    "Climate science models integrate atmospheric, oceanic, and terrestrial \
     data to project future environmental conditions with increasing accuracy. \
     Ensemble methods and high-resolution simulations help policymakers \
     understand risks and plan adaptation strategies.",
    "Robotic systems in healthcare settings assist surgeons with precision \
     procedures and support rehabilitation through adaptive therapy programs. \
     Advances in haptic feedback and computer vision enable safer and more \
     effective human-robot collaboration in clinical environments.",
    "Digital humanities scholarship applies computational methods to literary, \
     historical, and cultural analysis. Text mining, network visualization, \
     and geospatial mapping tools reveal patterns in archives and collections \
     that would be impossible to identify through manual review alone.",
];

/// Extra vocabulary appended to each generated paragraph to ensure document
/// uniqueness. Thirty-four words. Sample uniformly.
pub const EXTRA_VOCAB: &[&str] = &[
    "performance",
    "optimization",
    "distributed",
    "concurrent",
    "scalable",
    "efficient",
    "robust",
    "innovative",
    "comprehensive",
    "fundamental",
    "architecture",
    "infrastructure",
    "methodology",
    "implementation",
    "evaluation",
    "framework",
    "algorithm",
    "protocol",
    "specification",
    "integration",
    "verification",
    "validation",
    "deployment",
    "monitoring",
    "analysis",
    "synthesis",
    "transformation",
    "processing",
    "computation",
    "visualization",
    "simulation",
    "approximation",
    "calibration",
    "aggregation",
];

/// Generate a unique paragraph by sampling a template and appending
/// `2..=4` extra-vocabulary keywords. Lifted from `fulltext_query.rs`.
pub fn random_paragraph(rng: &mut impl Rng) -> String {
    let template = PARAGRAPH_TEMPLATES[rng.gen_range(0..PARAGRAPH_TEMPLATES.len())];
    let n_extra = rng.gen_range(2..=4);
    let extras: Vec<&str> = (0..n_extra)
        .map(|_| EXTRA_VOCAB[rng.gen_range(0..EXTRA_VOCAB.len())])
        .collect();
    format!("{} Keywords: {}.", template, extras.join(", "))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn paragraph_is_seed_deterministic() {
        let p1 = random_paragraph(&mut StdRng::seed_from_u64(42));
        let p2 = random_paragraph(&mut StdRng::seed_from_u64(42));
        assert_eq!(p1, p2);
    }

    #[test]
    fn different_seeds_diverge() {
        let p1 = random_paragraph(&mut StdRng::seed_from_u64(0));
        let p2 = random_paragraph(&mut StdRng::seed_from_u64(1));
        assert_ne!(p1, p2);
    }

    #[test]
    fn paragraph_has_keywords_section() {
        let p = random_paragraph(&mut StdRng::seed_from_u64(42));
        assert!(
            p.contains("Keywords:"),
            "missing Keywords: section in {p:?}"
        );
    }

    #[test]
    fn templates_and_vocab_nonempty() {
        assert!(!PARAGRAPH_TEMPLATES.is_empty());
        assert!(!EXTRA_VOCAB.is_empty());
    }
}
