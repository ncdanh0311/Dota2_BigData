# Dữ liệu

CSV nguồn lấy từ https://zenodo.org/records/23166182, phiên bản v3.
Không commit CSV hoặc Parquet vào repo. Dữ liệu nằm trong HDFS:

- Raw: `/dota/raw/zenodo_v3/DB_03_Live_Features_ANALYSIS_v3.csv`.
- Processed: `/dota/processed/match_drafts_v1/`.

`reference/source_manifest.json` chứa URL, checksum, dung lượng, header và số
dòng nguồn đã kiểm chứng. Pipeline đọc file này làm tham chiếu schema.
Lệnh `python3 scripts/hdfs_cluster_manager.py download` cập nhật manifest sau khi file tải
khớp MD5 công bố. Xem hướng dẫn HDFS trong `docs/` để tải và nạp dữ liệu.
