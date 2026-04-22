mod tests {
    use fluree_db_indexer::hll::HllSketch256;

    #[test]
    fn rejects_invalid_versioned_bytes() {
        let sketch = HllSketch256::new();
        let mut bytes = sketch.to_bytes_versioned();

        // Wrong length
        assert!(HllSketch256::from_bytes_versioned(&bytes[..100]).is_none());

        // Unsupported version
        bytes[0] = 2;
        assert!(HllSketch256::from_bytes_versioned(&bytes).is_none());
    }
}
