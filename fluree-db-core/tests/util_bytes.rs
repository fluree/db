//! Bytes utility tests
//!
//! Byte utilities tests.
//! Tests byte conversion utilities for IRI encoding/decoding.
//!
//! These utilities convert between UTF-8 strings and sequences of i64 values
//! by splitting the byte representation into 8-byte chunks and interpreting
//! each chunk as a big-endian i64.

#[cfg(test)]
mod tests {
    // Implementation of byte conversion functions equivalent to
    // fluree.db.util.bytes/string->UTF8, UTF8->long, long->UTF8, UTF8->string

    fn string_to_utf8(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    fn utf8_to_long(bytes: &[u8]) -> i64 {
        // Convert up to 8 bytes to a big-endian i64
        // Pad with zeros if less than 8 bytes
        let mut buf = [0u8; 8];
        let len = bytes.len().min(8);
        buf[..len].copy_from_slice(&bytes[..len]);
        i64::from_be_bytes(buf)
    }

    fn long_to_utf8(value: i64) -> Vec<u8> {
        // Convert i64 to big-endian bytes
        value.to_be_bytes().to_vec()
    }

    fn utf8_to_string(bytes: &[u8]) -> String {
        String::from_utf8(bytes.to_vec()).unwrap_or_default()
    }

    fn string_to_longs(s: &str) -> Vec<i64> {
        string_to_utf8(s).chunks(8).map(utf8_to_long).collect()
    }

    fn longs_to_string(longs: &[i64]) -> String {
        let mut bytes: Vec<u8> = longs.iter().flat_map(|&l| long_to_utf8(l)).collect();

        // Remove trailing null bytes that were added as padding
        while let Some(&0) = bytes.last() {
            bytes.pop();
        }

        utf8_to_string(&bytes)
    }

    #[test]
    fn iri_longs_roundtrip() {
        // Test that IRI strings can be converted to longs and back
        // Mirrors: iri->longs-roundtrip test with negative longs handling
        let test_string = "Permian–Triassic_extinction_event";

        let longs = string_to_longs(test_string);
        let result = longs_to_string(&longs);

        assert_eq!(result, test_string);
    }

    #[test]
    fn iri_roundtrip_property() {
        // Test roundtrip property for various IRI strings
        // Equivalent to property-based testing via generators

        // Test a comprehensive set of IRI examples
        let test_cases = vec![
            "http://example.org/test",
            "urn:test:example",
            "https://www.w3.org/2001/XMLSchema#string",
            "mailto:test@example.com",
            "ftp://ftp.example.com/file.txt",
            "file:///home/user/document.txt",
            "ldap://ldap.example.com/dc=example,dc=com",
            "news:comp.lang.rust",
            "tel:+1-555-123-4567",
            "urn:ietf:rfc:3986",
            "tag:example.com,2023:blog-post",
            // Test edge cases
            "a:b", // minimal IRI
            "scheme:path",
            "scheme://host/path?query=value#fragment",
            // Test Unicode characters
            "http://example.com/tëst",
            "http://例え.テスト/path",
        ];

        for iri in test_cases {
            let longs = string_to_longs(iri);
            let result = longs_to_string(&longs);
            assert_eq!(result, iri, "Roundtrip failed for IRI: {iri}");
        }
    }

    #[test]
    fn bytes_conversion_edge_cases() {
        // Test edge cases for byte conversion

        // Empty string
        assert_eq!(longs_to_string(&string_to_longs("")), "");

        // Single character
        assert_eq!(longs_to_string(&string_to_longs("a")), "a");

        // String shorter than 8 bytes
        let short = "hello";
        assert_eq!(longs_to_string(&string_to_longs(short)), short);

        // String exactly 8 bytes
        let eight_bytes = "12345678";
        assert_eq!(longs_to_string(&string_to_longs(eight_bytes)), eight_bytes);

        // String longer than 8 bytes
        let long = "this is a longer string that spans multiple chunks";
        assert_eq!(longs_to_string(&string_to_longs(long)), long);

        // Test individual byte conversion functions
        let test_bytes = b"hello";
        let long_val = utf8_to_long(test_bytes);
        let back_to_bytes = long_to_utf8(long_val);
        // Only compare the first 5 bytes since the rest are padded with zeros
        assert_eq!(&back_to_bytes[..test_bytes.len()], test_bytes);
    }
}
