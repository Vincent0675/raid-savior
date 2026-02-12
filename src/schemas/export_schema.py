#!/usr/bin/env python3
"""
Export formal JSON Schema from Pydantic models.
Interoperable contract for documentation and external systems.

Phase: 1 (Schema + Synthetic Data Generation)
Output: schemas/eventos_schema.json
"""

from pathlib import Path
import json
from datetime import datetime

from src.schemas.eventos_schema import WoWRaidEvent


def main():
    print("=" * 70)
    print("WoW Telemetry Pipeline - JSON Schema Exporter")
    print("=" * 70)
    print()
    
    print("Extracting schema from Pydantic models...")
    
    # Generate JSON Schema with Pydantic v2
    schema = WoWRaidEvent.model_json_schema()
    
    # Schema info
    title = schema.get("title", "N/A")
    properties = schema.get("properties", {})
    required = schema.get("required", [])
    
    print(f"  Title: {title}")
    print(f"  Properties: {len(properties)}")
    print(f"  Required fields: {len(required)}")
    print()
    
    # Show required fields
    print("Required fields:")
    for field in required[:10]:  # Show first 10
        field_info = properties.get(field, {})
        field_type = field_info.get("type", "unknown")
        print(f"  - {field}: {field_type}")
    if len(required) > 10:
        print(f"  ... and {len(required) - 10} more")
    print()
    
    # Prepare output
    out_path = Path("schemas/eventos_schema.json")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Write JSON Schema
    print("Writing JSON Schema...")
    out_path.write_text(
        json.dumps(schema, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    
    # File stats
    file_size_kb = out_path.stat().st_size / 1024
    
    print()
    print("=" * 70)
    print("JSON SCHEMA EXPORTED SUCCESSFULLY")
    print("=" * 70)
    print(f"Path: {out_path}")
    print(f"Size: {file_size_kb:.2f} KB")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    print("Use cases:")
    print("  - API documentation (OpenAPI/Swagger)")
    print("  - Validation in other languages (Java, Go, TypeScript)")
    print("  - Automatic client generation")
    print("  - Contract for external systems")
    print("=" * 70)


if __name__ == "__main__":
    main()