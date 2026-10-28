"""Airflow DAG entrypoint for the verified batch pipeline."""
from orchestration.pipeline_dag_definitions import build_batch
dag=build_batch()
