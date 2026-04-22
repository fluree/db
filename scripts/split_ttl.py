#!/usr/bin/env python3
"""Split a large Turtle (.ttl) file into smaller chunks.

Each chunk preserves prefix/base declarations from the original file
and splits only at statement boundaries (lines ending with '.').

Usage:
    python split_ttl.py /Volumes/External-4TB-OWCEnvoy/dblp/dblp.ttl --chunk-size 1024
    python split_ttl.py /Volumes/External-4TB-OWCEnvoy/dblp/dblp.ttl --num-chunks 25
"""

import argparse
import os
import sys
import time
from pathlib import Path


def parse_args():
    parser = argparse.ArgumentParser(description="Split a TTL file into chunks")
    parser.add_argument("input", help="Path to the input TTL file")
    parser.add_argument(
        "--output-dir", "-o",
        help="Output directory for chunks (default: <input_dir>/chunks)",
    )

    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--chunk-size", "-s", type=int, default=1024,
        help="Target chunk size in MB (default: 1024)",
    )
    group.add_argument(
        "--num-chunks", "-n", type=int,
        help="Number of chunks to split into (overrides --chunk-size)",
    )

    return parser.parse_args()


def split_ttl(input_path, output_dir, chunk_size_bytes):
    input_path = Path(input_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    file_size = input_path.stat().st_size
    print(f"Input file:  {input_path}")
    print(f"File size:   {file_size / (1024**3):.2f} GB")
    print(f"Chunk target: {chunk_size_bytes / (1024**2):.0f} MB")
    print(f"Est. chunks: ~{max(1, file_size // chunk_size_bytes)}")
    print()

    prefixes = []
    chunk_num = 0
    current_bytes = 0
    current_lines = []
    in_prefixes = True
    total_bytes_read = 0
    t0 = time.time()

    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            line_bytes = len(line.encode("utf-8"))
            total_bytes_read += line_bytes

            # Collect prefix/base declarations at the top of the file
            if in_prefixes:
                if stripped.startswith(("@prefix", "@base", "PREFIX", "BASE")) or stripped == "":
                    prefixes.append(line)
                    continue
                else:
                    in_prefixes = False
                    print(f"Collected {len(prefixes)} prefix/header lines")
                    print()

            current_lines.append(line)
            current_bytes += line_bytes

            # Split when over target size AND at a statement boundary
            if current_bytes >= chunk_size_bytes and stripped.endswith("."):
                _write_chunk(output_dir, chunk_num, prefixes, current_lines, current_bytes)
                chunk_num += 1
                current_bytes = 0
                current_lines = []

                pct = total_bytes_read / file_size * 100
                elapsed = time.time() - t0
                rate = total_bytes_read / (1024**2) / elapsed if elapsed > 0 else 0
                print(f"    [{pct:5.1f}%]  {rate:.0f} MB/s  elapsed {elapsed:.0f}s")

    # Write remaining data
    if current_lines:
        _write_chunk(output_dir, chunk_num, prefixes, current_lines, current_bytes)
        chunk_num += 1

    elapsed = time.time() - t0
    print(f"\nDone. Wrote {chunk_num} chunks to {output_dir}  ({elapsed:.1f}s)")
    return chunk_num


def _write_chunk(output_dir, chunk_num, prefixes, lines, data_bytes):
    chunk_file = output_dir / f"chunk_{chunk_num:04d}.ttl"
    with open(chunk_file, "w", encoding="utf-8") as out:
        out.writelines(prefixes)
        if prefixes and not prefixes[-1].endswith("\n"):
            out.write("\n")
        out.write("\n")
        out.writelines(lines)

    prefix_bytes = sum(len(p.encode("utf-8")) for p in prefixes)
    total = prefix_bytes + data_bytes
    print(f"  chunk {chunk_num:4d}:  {total / (1024**2):8.1f} MB  ({chunk_file.name})")


def main():
    args = parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        print(f"Error: {input_path} not found", file=sys.stderr)
        sys.exit(1)

    output_dir = args.output_dir or str(input_path.parent / "chunks")

    if args.num_chunks:
        file_size = input_path.stat().st_size
        chunk_size_bytes = file_size // args.num_chunks
        print(f"Splitting into {args.num_chunks} chunks (~{chunk_size_bytes // (1024**2)} MB each)")
    else:
        chunk_size_bytes = args.chunk_size * 1024 * 1024

    split_ttl(input_path, output_dir, chunk_size_bytes)


if __name__ == "__main__":
    main()
