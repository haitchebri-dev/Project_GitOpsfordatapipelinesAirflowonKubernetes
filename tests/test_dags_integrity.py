from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import sys

from airflow.models import DagBag
import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
DAGS_DIR = REPO_ROOT / "dags"

if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from gitops_data_pipeline_dag import (  # noqa: E402
    extract_sample_data,
    load_orders,
    transform_orders,
)

REQUIRED_TAGS = {"gitops", "airflow3"}


@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_no_import_errors(dag_bag: DagBag) -> None:
    """Block CI if any DAG has an import or syntax error."""
    assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"
    assert len(dag_bag.dags) >= 1, "No DAG found in dags/"


def test_no_cycles_in_dags(dag_bag: DagBag) -> None:
    """Block CI if any DAG contains a cycle."""
    for dag_id, dag in dag_bag.dags.items():
        dag.check_cycle()


def test_required_tags_and_retries(dag_bag: DagBag) -> None:
    """Verify required tags and task retry configuration."""
    for dag_id, dag in dag_bag.dags.items():
        dag_tags = set(dag.tags or [])
        assert REQUIRED_TAGS.issubset(dag_tags), (
            f"DAG '{dag_id}' must define tags {REQUIRED_TAGS}, got {dag_tags}"
        )
        for task in dag.tasks:
            retries = task.retries if task.retries is not None else dag.default_args.get("retries", 0)
            assert retries >= 1, f"Task '{task.task_id}' in '{dag_id}' must have retries >= 1"


def test_task_logic_and_dry_run_on_sample_data(dag_bag: DagBag) -> None:
    """Test task logic and run a DAG test on sample data."""
    sample = extract_sample_data()
    transformed = transform_orders(sample)
    assert len(transformed) == 2
    assert transformed[0] == {
        "order_id": 101,
        "amount_ht": 120.0,
        "amount_ttc": 144.0,
        "currency": "EUR",
    }

    summary = load_orders(transformed)
    assert summary == {"loaded_count": 2, "total_ttc": 444.6}

    dag = dag_bag.dags.get("gitops_data_pipeline_dag")
    assert dag is not None
    dag.test(logical_date=datetime(2026, 1, 2, tzinfo=timezone.utc))