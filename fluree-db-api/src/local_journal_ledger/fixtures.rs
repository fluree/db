// Exact archived Cypher corpus shared by unindexed and indexed adapter tests.
pub(super) const WRITES: &[(&str, &str)] = &[
    (
        "arango__single_edge_write",
        include_str!("fixtures/cypher/arango__single_edge_write.cypher"),
    ),
    (
        "arango__single_vertex_write",
        include_str!("fixtures/cypher/arango__single_vertex_write.cypher"),
    ),
    (
        "arango__unwind_range_vertex_write",
        include_str!("fixtures/cypher/arango__unwind_range_vertex_write.cypher"),
    ),
    (
        "create__edge",
        include_str!("fixtures/cypher/create__edge.cypher"),
    ),
    (
        "create__pattern",
        include_str!("fixtures/cypher/create__pattern.cypher"),
    ),
    (
        "create__vertex",
        include_str!("fixtures/cypher/create__vertex.cypher"),
    ),
    (
        "create__vertex_big",
        include_str!("fixtures/cypher/create__vertex_big.cypher"),
    ),
    (
        "update__vertex_on_property",
        include_str!("fixtures/cypher/update__vertex_on_property.cypher"),
    ),
];
pub(super) const OBSERVE: &[&str] = &[
    "MATCH (n) RETURN count(n)",
    "MATCH (n:User) RETURN count(n)",
    "MATCH (n:L1) RETURN count(n)",
    "MATCH ()-[r]->() RETURN count(r)",
    "MATCH (n:User {id:1}) RETURN n.id, n.age, n.property",
];
