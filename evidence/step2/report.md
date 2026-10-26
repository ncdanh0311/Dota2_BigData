# Báo cáo chất lượng dữ liệu — Bước 2

Trạng thái: **PASS**

| Chỉ tiêu | Kết quả |
|---|---:|
| source_rows | 1,007,768 |
| identified_matches | 90,530 |
| exact_duplicate_rows | 0 |
| invalid_source_rows | 0 |
| conflicting_matches | 0 |
| valid_matches | 90,530 |
| quarantined_matches | 0 |
| unidentifiable_source_rows | 0 |
| quarantined_unique_rows | 0 |

Nhãn trên bảng trận hợp lệ: `{'1': 46305, '0': 44225}`

Thời gian: 114.32 giây.

Đầu vào: `hdfs://localhost:9000/dota/raw/zenodo_v3/DB_03_Live_Features_ANALYSIS_v3.csv`

Đầu ra: `hdfs://localhost:9000/dota/processed/match_drafts_v1`

Đã đọc lại Parquet, kiểm tra kiểu dữ liệu, khóa duy nhất và 10 hero hợp lệ.
Bảng đầu ra chỉ có match_id, radiant_heroes, dire_heroes và radiant_win.
Không có patch/start_time trong nguồn; chưa chia tập và chưa huấn luyện ML.
