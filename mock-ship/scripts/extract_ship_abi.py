#!/usr/bin/env python3
"""
Extract the SHiP ABI JSON from Spring's abi.cpp source file.

Usage:
    python3 extract_ship_abi.py <path_to_abi.cpp> <output_path>

The ABI JSON is embedded in abi.cpp as a C++ raw string literal: R"({...})";
This script extracts the JSON content between R"( and )"; and validates it.
"""

import json
import sys


def extract_abi(cpp_path: str, output_path: str) -> None:
    with open(cpp_path, "r") as f:
        content = f.read()

    # Find the raw string literal boundaries
    start_marker = 'R"('
    end_marker = ')";'

    start_idx = content.find(start_marker)
    if start_idx == -1:
        print(f"ERROR: Could not find '{start_marker}' in {cpp_path}", file=sys.stderr)
        sys.exit(1)

    # Move past the marker to the actual JSON content
    json_start = start_idx + len(start_marker)

    end_idx = content.find(end_marker, json_start)
    if end_idx == -1:
        print(f"ERROR: Could not find '{end_marker}' in {cpp_path}", file=sys.stderr)
        sys.exit(1)

    raw_json = content[json_start:end_idx]

    # Validate it's proper JSON
    try:
        parsed = json.loads(raw_json)
    except json.JSONDecodeError as e:
        print(f"ERROR: Extracted content is not valid JSON: {e}", file=sys.stderr)
        print(f"First 200 chars: {raw_json[:200]}", file=sys.stderr)
        sys.exit(1)

    # Verify expected structure
    assert "version" in parsed, "Missing 'version' field"
    assert "structs" in parsed, "Missing 'structs' field"
    assert "variants" in parsed, "Missing 'variants' field"

    # Find the request/result variants to verify protocol compatibility
    variant_names = {v["name"] for v in parsed["variants"]}
    assert "request" in variant_names, "Missing 'request' variant"
    assert "result" in variant_names, "Missing 'result' variant"

    # Write the validated JSON (pretty-printed for readability)
    with open(output_path, "w") as f:
        json.dump(parsed, f, indent=4)

    print(f"OK: Extracted ABI v{parsed['version']}")
    print(f"    {len(parsed['structs'])} structs, {len(parsed['variants'])} variants")
    print(f"    Written to {output_path} ({len(raw_json)} bytes)")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <abi.cpp path> <output.json path>")
        sys.exit(1)

    extract_abi(sys.argv[1], sys.argv[2])
