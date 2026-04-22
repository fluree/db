#!/usr/bin/env python3
"""
Converts a Fluree commit-chain database (JSON-LD) to deduplicated Turtle files.

Scans all DB data files in the commit directory, extracts f:assert entries,
deduplicates triples, and outputs Turtle in ~1GB chunks.

Usage:
    python3 fluree_to_turtle.py
"""

import json
import os
import sys
import time
from collections import defaultdict

COMMIT_DIR = '/Volumes/External-4TB-OWCEnvoy/aj-cn2/fluree-jld/369435906935887/commit'
OUTPUT_DIR = '/Volumes/External-4TB-OWCEnvoy/aj-cn2/fluree-jld/369435906935887/turtle_output'
CHUNK_SIZE_BYTES = 1_000_000_000  # 1GB per chunk

# Common prefixes for the Turtle header
TURTLE_PREFIXES = """\
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix prov: <http://www.w3.org/ns/prov#> .
@prefix skos: <http://www.w3.org/2004/02/skos/core#> .
@prefix scoring: <http://www.mondeca.com/scoring#> .
@prefix publishing: <http://www.mondeca.com/system/publishing#> .
@prefix cntax: <https://taxonomy.condenast.com/model#> .
@prefix dblp: <https://dblp.org/rdf/schema#> .

"""


def escape_turtle_string(s):
    """Escape a string for Turtle literal format."""
    s = s.replace('\\', '\\\\')
    s = s.replace('"', '\\"')
    s = s.replace('\n', '\\n')
    s = s.replace('\r', '\\r')
    s = s.replace('\t', '\\t')
    return s


def format_uri(uri):
    """Format a URI for Turtle output, using angle brackets."""
    # Escape special chars in URIs per Turtle spec
    uri = uri.replace('>', '\\>')
    return f'<{uri}>'


def format_subject(node_id):
    """Format a subject node (URI or blank node)."""
    if node_id.startswith('_:'):
        return node_id
    return format_uri(node_id)


def format_object_value(value):
    """
    Convert a JSON-LD value to Turtle object representation(s).
    Returns a list of formatted strings (one per triple object).
    """
    if isinstance(value, dict):
        if '@id' in value:
            return [format_subject(value['@id'])]
        if '@value' in value:
            val_str = escape_turtle_string(str(value['@value']))
            dtype = value.get('@type')
            lang = value.get('@language')
            if dtype:
                return [f'"{val_str}"^^{format_uri(dtype)}']
            if lang:
                return [f'"{val_str}"@{lang}']
            return [f'"{val_str}"']
        # Fallback for other dict patterns
        return [f'"{escape_turtle_string(json.dumps(value))}"']

    if isinstance(value, list):
        results = []
        for item in value:
            results.extend(format_object_value(item))
        return results

    if isinstance(value, bool):
        v = 'true' if value else 'false'
        return [f'"{v}"^^<http://www.w3.org/2001/XMLSchema#boolean>']

    if isinstance(value, float):
        return [f'"{value}"^^<http://www.w3.org/2001/XMLSchema#double>']

    if isinstance(value, int):
        return [f'"{value}"^^<http://www.w3.org/2001/XMLSchema#integer>']

    if isinstance(value, str):
        return [f'"{escape_turtle_string(value)}"']

    return [f'"{escape_turtle_string(str(value))}"']


def extract_triples(assert_obj):
    """
    Convert a single JSON-LD assert object into a set of
    (subject, predicate, object) triple strings.
    """
    triples = []
    subject_id = assert_obj.get('@id', '')
    if not subject_id:
        return triples

    subject = format_subject(subject_id)

    # Handle @type -> rdf:type
    types = assert_obj.get('@type', [])
    if isinstance(types, str):
        types = [types]
    for t in types:
        triples.append((subject, 'a', format_uri(t)))

    # Handle all other properties (skip JSON-LD keywords)
    for key, value in assert_obj.items():
        if key.startswith('@'):
            continue
        predicate = format_uri(key)
        for obj_str in format_object_value(value):
            triples.append((subject, predicate, obj_str))

    return triples


def main():
    start_time = time.time()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Phase 1: Scan all DB data files, extract and deduplicate triples
    # Use a dict of subject -> set of (predicate, object) for dedup + grouping
    subject_data = defaultdict(set)
    triple_count = 0
    db_file_count = 0
    error_count = 0

    files = [f for f in os.listdir(COMMIT_DIR) if f.endswith('.json')]
    total_files = len(files)

    print(f"Scanning {total_files} JSON files in commit directory...")
    print(f"Output directory: {OUTPUT_DIR}")
    print()

    for i, fname in enumerate(files):
        path = os.path.join(COMMIT_DIR, fname)
        try:
            with open(path) as f:
                data = json.load(f)
        except Exception as e:
            error_count += 1
            if error_count <= 10:
                print(f"  Warning: failed to read {fname}: {e}", file=sys.stderr)
            continue

        # Skip commit wrapper files (only process DB data files with asserts)
        if 'f:assert' not in data:
            continue

        db_file_count += 1

        asserts = data['f:assert']
        if not isinstance(asserts, list):
            asserts = [asserts]

        for assert_obj in asserts:
            for subj, pred, obj in extract_triples(assert_obj):
                subject_data[subj].add((pred, obj))
                triple_count += 1

        if (i + 1) % 5000 == 0:
            elapsed = time.time() - start_time
            unique = sum(len(po) for po in subject_data.values())
            print(
                f"  [{i+1:>6}/{total_files}] "
                f"{db_file_count} DB files, "
                f"{triple_count:,} raw triples, "
                f"{unique:,} unique, "
                f"{len(subject_data):,} subjects "
                f"({elapsed:.0f}s)"
            )

    unique_triples = sum(len(po) for po in subject_data.values())
    elapsed = time.time() - start_time
    print()
    print(f"Phase 1 complete in {elapsed:.1f}s:")
    print(f"  DB data files processed: {db_file_count:,}")
    print(f"  Raw triples extracted:   {triple_count:,}")
    print(f"  Unique triples:          {unique_triples:,}")
    print(f"  Unique subjects:         {len(subject_data):,}")
    if error_count:
        print(f"  Files with errors:       {error_count}")
    print()

    # Phase 2: Write Turtle output in chunks
    print("Phase 2: Writing Turtle output...")

    chunk_num = 0
    current_size = 0
    outfile = None

    def open_chunk():
        nonlocal chunk_num, current_size, outfile
        path = os.path.join(OUTPUT_DIR, f'{chunk_num:02d}.ttl')
        outfile = open(path, 'w', encoding='utf-8')
        outfile.write(TURTLE_PREFIXES)
        current_size = len(TURTLE_PREFIXES.encode('utf-8'))
        print(f"  Writing {chunk_num:02d}.ttl ...")

    def close_chunk():
        nonlocal chunk_num, outfile
        if outfile:
            outfile.close()
            path = os.path.join(OUTPUT_DIR, f'{chunk_num:02d}.ttl')
            size_mb = os.path.getsize(path) / (1024 * 1024)
            print(f"  Closed {chunk_num:02d}.ttl ({size_mb:.1f} MB)")
            chunk_num += 1

    open_chunk()

    subjects_written = 0
    for subject, po_set in subject_data.items():
        # Build the Turtle block for this subject
        po_list = sorted(po_set)
        lines = [subject]
        for i, (pred, obj) in enumerate(po_list):
            sep = ' ;' if i < len(po_list) - 1 else ' .'
            lines.append(f'    {pred} {obj}{sep}')
        block = '\n'.join(lines) + '\n\n'
        block_bytes = len(block.encode('utf-8'))

        # Check if we need a new chunk (but don't leave an empty chunk)
        if current_size + block_bytes > CHUNK_SIZE_BYTES and current_size > len(TURTLE_PREFIXES.encode('utf-8')):
            close_chunk()
            open_chunk()

        outfile.write(block)
        current_size += block_bytes
        subjects_written += 1

        if subjects_written % 100000 == 0:
            print(f"    {subjects_written:,} subjects written...")

    close_chunk()

    total_elapsed = time.time() - start_time
    print()
    print(f"Done in {total_elapsed:.1f}s!")
    print(f"  {chunk_num} Turtle file(s) written to: {OUTPUT_DIR}")
    print(f"  {unique_triples:,} unique triples across {len(subject_data):,} subjects")


if __name__ == '__main__':
    main()
