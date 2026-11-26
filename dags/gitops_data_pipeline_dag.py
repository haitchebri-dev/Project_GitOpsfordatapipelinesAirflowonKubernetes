from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from airflow.sdk import dag, task


def extract_sample_data() -> list[dict[str, Any]]:
    """Simule l'extraction d'un lot de données brutes."""
    return [
        {"order_id": 101, "amount": 120.0, "currency": "EUR", "valid": True},
        {"order_id": 102, "amount": -15.0, "currency": "EUR", "valid": False},
        {"order_id": 103, "amount": 250.5, "currency": "EUR", "valid": True},
    ]


def transform_orders(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Filtre les commandes invalides et applique la TVA (20 %)."""
    cleaned: list[dict[str, Any]] = []
    for row in records:
        if not row.get("valid") or float(row.get("amount", 0)) <= 0:
            continue
        amount_ht = float(row["amount"])
        cleaned.append(
            {
                "order_id": int(row["order_id"]),
                "amount_ht": round(amount_ht, 2),
                "amount_ttc": round(amount_ht * 1.20, 2),
                "currency": str(row.get("currency", "EUR")),
            }
        )
    return cleaned


def load_orders(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Prépare le résumé de chargement."""
    total_ttc = round(sum(r["amount_ttc"] for r in records), 2)
    summary = {"loaded_count": len(records), "total_ttc": total_ttc}
    print(f"Loaded summary: {summary}")
    return summary


@dag(
    dag_id="gitops_data_pipeline_dag",
    description="Pipeline ETL GitOps déployé sur Airflow 3 / Kubernetes",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["gitops", "airflow3", "k3d", "etl"],
    default_args={
        "owner": "hajar-haytam",
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
)
def gitops_data_pipeline() -> None:
    @task
    def extract() -> list[dict[str, Any]]:
        return extract_sample_data()

    @task
    def transform(raw_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return transform_orders(raw_records)

    @task
    def load(transformed_records: list[dict[str, Any]]) -> dict[str, Any]:
        return load_orders(transformed_records)

    raw = extract()
    cleaned = transform(raw)
    load(cleaned)


dag = gitops_data_pipeline()