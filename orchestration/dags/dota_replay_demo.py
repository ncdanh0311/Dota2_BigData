"""Airflow DAG entrypoint for bounded historical Kafka replay."""
from orchestration.definitions import build_replay
dag=build_replay()
