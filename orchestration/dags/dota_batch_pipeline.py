"""Airflow DAG entrypoint for the verified batch pipeline."""
from orchestration.definitions import build_batch
dag=build_batch()
