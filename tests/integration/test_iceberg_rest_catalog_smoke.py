"""Smoke tests para flujo MinIO -> REST Catalog -> visibilidad de tablas."""

import requests
import pytest

pyiceberg = pytest.importorskip("pyiceberg.catalog")
load_catalog = pyiceberg.load_catalog


REST_URI = "http://localhost:8181"
EXPECTED_TABLES = {
    "gold.dim_player",
    "gold.dim_raid",
    "gold.fact_player_raid_stats",
    "gold.fact_raid_summary",
    "silver.raid_events",
}


def _catalog_properties() -> dict[str, str]:
    return {
        "type": "rest",
        "uri": REST_URI,
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://localhost:9000",
        "s3.access-key-id": "minio",
        "s3.secret-access-key": "minio123",
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }


def _build_table_set(catalog) -> set[str]:
    table_names: set[str] = set()
    for namespace in catalog.list_namespaces():
        ns_name = namespace[0] if isinstance(namespace, tuple) else namespace
        for table in catalog.list_tables(ns_name):
            if isinstance(table, tuple):
                table_names.add(".".join(table))
            else:
                table_names.add(str(table))
    return table_names


def test_rest_catalog_config_endpoint_available():
    try:
        response = requests.get(f"{REST_URI}/v1/config", timeout=5)
    except requests.RequestException:
        pytest.skip("REST Catalog no disponible en localhost:8181")

    assert response.status_code == 200


def test_tables_visible_through_pyiceberg_catalog():
    try:
        catalog = load_catalog("wow", **_catalog_properties())
        discovered = _build_table_set(catalog)
    except Exception:
        pytest.skip("No se pudo inicializar PyIceberg contra REST Catalog")

    missing = EXPECTED_TABLES - discovered
    assert not missing, f"Tablas no visibles en REST Catalog: {sorted(missing)}"
