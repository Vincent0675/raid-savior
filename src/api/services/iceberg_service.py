from __future__ import annotations

import json
import logging
from typing import Any
from typing import cast

import pandas as pd
import requests
from pyiceberg.catalog import load_catalog

from src.api.settings import get_settings

logger = logging.getLogger(__name__)


class IcebergService:
    def __init__(self) -> None:
        self.catalog_name = "wow"

    @staticmethod
    def _settings():
        return get_settings()

    @staticmethod
    def _records(dataframe: pd.DataFrame) -> list[dict[str, Any]]:
        if dataframe.empty:
            return []
        return cast(
            list[dict[str, Any]],
            json.loads(dataframe.to_json(orient="records", date_format="iso")),
        )

    def _catalog(self):
        settings = self._settings()
        catalog_name = getattr(settings, "iceberg_catalog_name", self.catalog_name)
        return catalog_name, load_catalog(catalog_name, **self._catalog_properties())

    def _table_to_dataframe(self, namespace: str, table_name: str) -> pd.DataFrame:
        _, catalog = self._catalog()
        table = catalog.load_table((namespace, table_name))
        return table.scan().to_arrow().to_pandas()

    def _expected_tables(self) -> set[str]:
        settings = self._settings()
        return {
            table.strip()
            for table in settings.iceberg_expected_tables.split(",")
            if table.strip()
        }

    def _catalog_properties(self) -> dict[str, str]:
        settings = self._settings()
        return {
            "type": "rest",
            "uri": settings.iceberg_rest_uri,
            "warehouse": f"s3://{settings.warehouse_bucket}/",
            "s3.endpoint": settings.s3_endpoint_url,
            "s3.access-key-id": settings.s3_access_key,
            "s3.secret-access-key": settings.s3_secret_key,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        }

    def _validate_rest_catalog(self) -> None:
        settings = self._settings()
        response = requests.get(
            f"{settings.iceberg_rest_uri}/v1/config",
            timeout=settings.readiness_timeout_seconds,
        )
        response.raise_for_status()

    def _list_catalog_tables(self) -> set[str]:
        _, catalog = self._catalog()
        namespaces = catalog.list_namespaces()
        discovered_tables: set[str] = set()

        for namespace in namespaces:
            namespace_name = namespace[0] if isinstance(namespace, tuple) else namespace
            for table in catalog.list_tables(namespace_name):
                if isinstance(table, tuple):
                    discovered_tables.add(".".join(table))
                else:
                    discovered_tables.add(str(table))

        return discovered_tables

    def list_catalog_tables(self) -> list[str]:
        return sorted(self._list_catalog_tables())

    def list_namespaces(self) -> list[str]:
        _, catalog = self._catalog()
        namespaces: list[str] = []
        for namespace in catalog.list_namespaces():
            namespaces.append(namespace[0] if isinstance(namespace, tuple) else namespace)
        return sorted(namespaces)

    def list_raids(self, limit: int = 50, offset: int = 0) -> dict[str, Any]:
        limit = max(1, min(limit, 500))
        offset = max(0, offset)
        raids_df = self._table_to_dataframe("gold", "fact_raid_summary")

        if not raids_df.empty:
            sort_columns: list[str] = []
            if "event_date" in raids_df.columns:
                sort_columns.append("event_date")
            if "raid_id" in raids_df.columns:
                sort_columns.append("raid_id")
            if sort_columns:
                raids_df = raids_df.sort_values(by=sort_columns, ascending=[False] * len(sort_columns))

        total = int(len(raids_df))
        page_df = raids_df.iloc[offset : offset + limit]
        return {
            "raids": self._records(page_df),
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    def get_raid_summary(self, raid_id: str) -> dict[str, Any] | None:
        raids_df = self._table_to_dataframe("gold", "fact_raid_summary")
        if "raid_id" not in raids_df.columns:
            return None

        filtered = raids_df[raids_df["raid_id"] == raid_id]
        if filtered.empty:
            return None
        return self._records(filtered.iloc[:1])[0]

    def list_raid_players(self, raid_id: str) -> list[dict[str, Any]]:
        players_df = self._table_to_dataframe("gold", "fact_player_raid_stats")
        if "raid_id" not in players_df.columns:
            return []

        filtered = players_df[players_df["raid_id"] == raid_id]
        if "damage_total" in filtered.columns:
            filtered = filtered.sort_values(by=["damage_total"], ascending=[False])
        return self._records(filtered)

    def get_global_metrics(self) -> dict[str, Any]:
        raids_df = self._table_to_dataframe("gold", "fact_raid_summary")
        total_raids = int(len(raids_df))

        success_raids = 0
        wipe_raids = 0
        if "raid_outcome" in raids_df.columns:
            success_raids = int((raids_df["raid_outcome"] == "success").sum())
            wipe_raids = int((raids_df["raid_outcome"] == "wipe").sum())

        wipe_rate_pct = float((wipe_raids / total_raids) * 100.0) if total_raids > 0 else 0.0
        avg_raid_dps = float(raids_df["raid_dps"].mean()) if "raid_dps" in raids_df.columns and total_raids > 0 else 0.0

        return {
            "total_raids": total_raids,
            "success_raids": success_raids,
            "wipe_raids": wipe_raids,
            "wipe_rate_pct": wipe_rate_pct,
            "avg_raid_dps": avg_raid_dps,
        }

    def check_readiness(self) -> dict[str, Any]:
        errors: list[str] = []
        discovered_tables: set[str] = set()
        expected_tables = self._expected_tables()

        try:
            self._validate_rest_catalog()
        except Exception as exc:  # pragma: no cover - defensivo
            errors.append(f"REST catalog no accesible: {exc}")

        if not errors:
            try:
                discovered_tables = self._list_catalog_tables()
            except Exception as exc:  # pragma: no cover - defensivo
                errors.append(f"No se pudieron listar tablas del catálogo: {exc}")

        missing_tables = sorted(expected_tables - discovered_tables)
        if missing_tables:
            errors.append(
                "Tablas Iceberg faltantes en catálogo REST: "
                + ", ".join(missing_tables)
            )

        if errors:
            logger.error("Readiness Iceberg fallida: %s", errors)
        else:
            logger.info(
                "Readiness Iceberg OK con %s tablas detectadas", len(discovered_tables)
            )

        settings = self._settings()
        return {
            "ready": not errors,
            "catalog_uri": settings.iceberg_rest_uri,
            "expected_tables": sorted(expected_tables),
            "catalog_tables": sorted(discovered_tables),
            "missing_tables": missing_tables,
            "errors": errors,
        }
