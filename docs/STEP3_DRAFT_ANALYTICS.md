# Bước 3 — Phân tích đội hình bằng Spark SQL

> Các đường dẫn evidence là output local bị Git bỏ qua. Tải báo cáo/ảnh từ [Actions Artifacts](EVIDENCE_DOWNLOADS.md).

## Chạy từ PowerShell

```powershell
cd D:\BigDataTH_Project\Dota2_BigData
.\commands\start-hadoop.ps1
.\commands\analyze-hero-statistics.ps1
```

Mỗi lần chạy mặc định tạo run ID UTC kèm hậu tố ngẫu nhiên. Có thể chỉ định:

```powershell
.\commands\analyze-hero-statistics.ps1 -RunId demo-analysis-01 -MinHeroMatches 100 -MinPairMatches 100
```

Các tham số khác: `-InputPath` và `-OutputRoot`. CLI Python tương ứng nhận
`--input`, `--output-root`, `--run-id`, `--min-hero-matches`, `--min-pair-matches`.
Ngưỡng phải là số nguyên dương; run ID là một tên thư mục, không chứa slash.
Không ghi đè run HDFS hoặc thư mục evidence đã có; dùng run ID mới để chạy lại.

Script dùng `configs/hdfs-wsl.json` và bridge WSL hiện có: Spark `local[2]`,
heap 1 GB, shuffle partitions 8, driver bind `127.0.0.1`. Chưa dùng YARN.

## Dữ liệu và quy tắc

Đầu vào mặc định:

```text
hdfs://localhost:9000/dota/processed/match_drafts_v1/
```

Job kiểm tra đúng bốn cột Silver, kiểu dữ liệu, khóa duy nhất, ID dương,
mỗi đội năm hero và tổng mười hero khác nhau, nhãn 0/1, input không rỗng.
Input không hợp lệ làm job dừng; không sửa hoặc lọc Silver âm thầm.
Thứ tự hero được chuẩn hóa bằng `array_sort` khi tính toán.

Các temporary view do Spark SQL tạo:

| View | Số dòng |
|---|---:|
| matches | N |
| team_matches | 2N |
| hero_appearances | 10N |
| pair_appearances | 20N |

Radiant dùng `team_win=radiant_win`; Dire dùng `team_win=1-radiant_win`.
Hai lần `posexplode` diễn ra trên mảng của **cùng một đội**, lọc `i<j` để tạo
đúng mười cặp/đội. Không join Descartes toàn bộ trận, không dùng Python UDF.

Các công thức dùng N thực tế:

```text
hero_pick_rate = picks / N
hero_win_rate = wins / picks
side_pick_rate = picks_on_side / N
side_win_rate = wins_on_side / picks_on_side
side_baseline = wins_of_side / N
delta_vs_side_baseline = side_win_rate - side_baseline
pair_pick_rate = pair_matches / N
pair_win_rate = pair_wins / pair_matches
```

`try_divide` trả null khi mẫu số 0. Tỷ lệ lưu Double 0–1; báo cáo định dạng
phần trăm. Delta nhân 100 là điểm phần trăm. Tổng hero pick rate bằng 10,
không bằng 1, vì mỗi trận có mười hero.

Hero, hero theo bên và cặp dưới ngưỡng vẫn được giữ với
`sample_status=insufficient_sample`; ngưỡng đủ mẫu là `eligible`.
Top win rate chỉ lấy `eligible`; top lượt chọn vẫn kèm sample status.
Không tạo phía còn thiếu bằng số 0. So sánh hai bên dùng `not_comparable` nếu
thiếu bên, `insufficient_sample` nếu một bên thiếu mẫu, còn lại là `eligible`.

## Hợp đồng Gold

```text
/dota/analytics/draft_stats_v1/<analysis_run_id>/
├── match_overview/
├── hero_stats/
├── hero_side_stats/
├── hero_pair_stats/
├── analysis_runs/
├── report.json
├── quality_checks.json
├── samples.json
├── report.md
└── _COMPLETED
```

Mỗi bảng có `analysis_run_id`, ghi một file Parquet Snappy, không partition
theo match hoặc hero ID.

| Bảng | Khóa | Nội dung |
|---|---|---|
| match_overview | analysis_run_id | total_matches, radiant_wins, dire_wins, hai win rate |
| hero_stats | hero_id | picks, wins, losses, pick_rate, win_rate, sample_status |
| hero_side_stats | hero_id + side | picks, wins, losses, side_pick_rate, side_win_rate, side_baseline, delta_vs_side_baseline, sample_status |
| hero_pair_stats | hero_a + hero_b | pair_matches, pair_wins, pair_losses, pair_pick_rate, pair_win_rate, sample_status |
| analysis_runs | analysis_run_id | nguồn/version, file inventory, fingerprint, Git commit/dirty flag, logic SHA-256, Spark version, N, coverage, ngưỡng, số dòng, checks, thời gian, giới hạn |

Các trường `*_json` trong `analysis_runs` chứa chuỗi JSON để lưu metadata
có cấu trúc mà schema Parquet vẫn ổn định. Inventory ghi URI, byte, mtime và
HDFS file checksum của từng Parquet nguồn. `git_dirty` và SHA-256 file logic
giúp truy vết khi chạy trước khi commit mã mới.

## Công bố run và bằng chứng

Khi bắt đầu ghi, job giữ `_RUNNING`. Mỗi bảng nghiệp vụ được đọc lại, so sánh
kiểu dữ liệu, khóa, số dòng và toàn bộ giá trị theo khóa. Double có sai số
tối đa 1e-9. Bảng metadata cũng được kiểm tra round-trip.

Sau khi đọc lại đạt, job xuất báo cáo local và copy lên run HDFS. Chỉ sau khi
mọi bước đó thành công mới đổi `_RUNNING` thành `_COMPLETED`. File `_SUCCESS`
trong từng thư mục bảng không thay thế dấu hoàn tất cấp run.

Nếu lỗi, giữ dữ liệu trung gian để chẩn đoán và ghi `failure.json` local,
`_FAILED.json` trên HDFS khi có thể. Run không có `_COMPLETED` không được xem
là đã công bố, dù có một vài bảng hoặc báo cáo PASS từ kiểm tra trung gian.

Bản báo cáo local nằm trong `evidence/step3/<analysis_run_id>/`:

- `report.json` / `report.md`: tổng quan và các bảng top 20.
- `quality_checks.json`: tổng đối soát và số dòng bảng.
- `samples.json`: số liệu top 20, so sánh năm hero được chọn nhiều nhất.

Tie-break: chỉ số chính giảm dần, số trận giảm dần, hero ID tăng dần. Hero/cặp
hiển thị bằng `Hero <id>`, không tải metadata tên hoặc suy diễn patch.

## Kiểm thử

Chạy cả normalization và analytics bằng:

```powershell
.\scripts\run-tests.ps1
```

Analytics dùng 199 trận tính tay: 100 trận ở nhóm hero đầu và 99 trận ở nhóm
sau. Kiểm tra nhãn Dire, các tỷ lệ, ngưỡng 99/100, giữ mẫu thiếu, hero chỉ có
một bên, so sánh hai bên, đảo thứ tự hero/partition, đọc lại Parquet, cố tình
làm sai output, input lỗi và run đã tồn tại. Không cần HDFS khi chạy fixture.
Kết quả tại `evidence/step3/rule_tests.json`. GitHub Actions chạy cùng fixture;
CI không chạy toàn bộ Silver hoặc tạo cụm HDFS.

Các invariant trên dữ liệu thật:

- Team/hero/pair appearances bằng 2N/10N/20N.
- Tổng hero picks/wins bằng 10N/5N; tổng pair_matches/pair_wins bằng 20N/10N.
- Radiant wins + Dire wins = N; tổng hai side win rate bằng 1.
- Hero tổng khớp tổng theo bên; wins + losses = số trận quan sát.
- Khóa Gold duy nhất, hero_a < hero_b; tỷ lệ hữu hạn và nằm trong khoảng hợp lệ.

## Giới hạn

Đây là mô tả lịch sử, không phải hiệu ứng phối hợp nhân quả hay dự đoán đội
hình mới. Không dùng Gold tính từ toàn bộ dữ liệu làm đặc trưng ML train/test.
Nguồn không có patch, timestamp, player/team identity hoặc tên hero. Coverage
100% chỉ có nghĩa toàn bộ Silver được xử lý, không phải toàn bộ trận Dota 2.

## Kết quả thực tế ngày 26/10/2026

Run đã công bố: `step3-20261026-v2`.

| Chỉ tiêu | Kết quả |
|---|---:|
| Trận đầu vào / đã xử lý | 90.530 / 90.530 |
| Hero được quan sát | 127 |
| Dòng hero theo bên | 254 |
| Cặp hero cùng đội khác nhau | 7.996 |
| Hero appearances | 905.300 |
| Pair appearances | 1.810.600 |
| Radiant thắng | 46.305 (51,15%) |
| Dire thắng | 44.225 (48,85%) |
| Tổng hero wins / pair wins | 452.650 / 905.300 |
| Coverage Silver | 100% |
| Thời gian xử lý, không gồm khởi động Spark | 36,80 giây |
| File Parquet nguồn được ghi inventory | 2 |

Cả năm bảng Gold đã được đọc lại và so sánh theo khóa, có đúng một file
Snappy cho mỗi bảng. Run `_COMPLETED` nằm tại:

```text
hdfs://localhost:9000/dota/analytics/draft_stats_v1/step3-20261026-v2/
```

Xem [báo cáo top hero/cặp](EVIDENCE_DOWNLOADS.md),
[quality checks](EVIDENCE_DOWNLOADS.md) và
[kiểm thử fixture](EVIDENCE_DOWNLOADS.md).

Run đầu `step3-20261026-v1` có số liệu thống kê đúng nhưng inventory rỗng do
Spark `inputFiles()` trả về rỗng sau cache. Đã bỏ marker hoàn tất, thay bằng
`_INVALIDATED` và lưu lý do. Code hiện liệt kê Parquet bằng Hadoop FS, bắt
buộc inventory không rỗng và có test hồi quy. Bản chẩn đoán v1 local nằm trong
`artifacts/step3/`, không commit. Không tiêu thụ v1 làm run đã công bố.
