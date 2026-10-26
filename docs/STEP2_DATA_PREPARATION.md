# Bước 2 — Chuẩn hóa đội hình Dota 2 bằng PySpark

## Mục tiêu

Đọc snapshot Zenodo v3 từ HDFS và tạo một bản ghi cho mỗi trận hợp lệ.
Parquet đầu ra phục vụ thống kê đội hình và ML trước trận; không chứa vàng,
kills, item, thời lượng hoặc các biến chỉ biết sau khi trận bắt đầu.
CSV gốc không thay đổi. Chưa chia train/test, chưa huấn luyện mô hình.

## Môi trường

- Ubuntu WSL, user `danh`; PySpark 4.2.0, Python 3.14.4, Java 17 đã có sẵn.
- `local[2]`, driver heap `1g`, shuffle partitions `8`.
- Đọc file theo partition tối đa 16 MiB, batch cache cột 1.000 dòng; cache
  snapshot và dữ liệu loại trùng trên đĩa để vừa heap 1 GB. Bảng tổng hợp trận
  nhỏ hơn dùng `MEMORY_AND_DISK`. Thư mục cache tạm do Spark quản lý trong WSL.
- HDFS config: `/home/danh/dota-hdfs/conf/namenode`.
- `SPARK_LOCAL_IP`, driver host và bind address dùng `127.0.0.1` để tránh
  WSL chọn nhầm địa chỉ loopback từ hostname.
- Spark dùng hai luồng local. Ba DataNode là lớp lưu trữ HDFS; không có nghĩa
  Spark chạy trên ba worker. Bước này không cần YARN hoặc Docker.

## Chạy từ PowerShell

```powershell
cd D:\BigDataTH_Project\Dota2_BigData
.\start-hadoop.ps1
.\run-step2.ps1
```

Nếu execution policy chặn, chạy riêng phiên lệnh với:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File D:\BigDataTH_Project\Dota2_BigData\run-step2.ps1
```

Chạy lại với output mới:

```powershell
.\run-step2.ps1 -OutputPath hdfs://localhost:9000/dota/processed/match_drafts_v2
```

Script cũng nhận `-InputPath` (một CSV HDFS, cùng header Zenodo v3).
Các đường dẫn được truyền làm đối số, không ghép thành lệnh shell.
Mặc định không ghi đè output, bảng quarantine hoặc JSON báo cáo đã tồn tại.
Nếu một lần chạy lỗi sau khi đã ghi output, giữ nguyên dữ liệu đó để kiểm tra
và chọn output mới cho lần chạy sau; không tự xóa thư mục HDFS.

## Pipeline

1. Kiểm tra manifest nguồn và header đủ 48 cột, kết nối HDFS và file đầu vào.
2. Đọc dạng dòng văn bản và parse bằng `from_csv` với schema cố định,
   không dùng `inferSchema`, không bỏ qua bản ghi lỗi.
3. Giữ `_raw_line` để đối chiếu và loại chính xác các dòng trùng hoàn toàn.
4. Kiểm tra từng snapshot; thống kê null của cả 48 cột, riêng các cột bắt buộc.
5. Sắp xếp riêng hero ID mỗi đội rồi kiểm tra mọi snapshot của từng trận.
6. Chỉ gom các trận không có vấn đề thành một dòng đội hình.
7. Ghi hai file Parquet Snappy, đọc lại bằng Spark và kiểm tra đầy đủ.
8. Ghi báo cáo local và sao chép JSON lên HDFS; chỉ thông báo PASS khi hoàn tất.

Nguồn có toàn bộ trường dạng số, một snapshot mỗi dòng. Pipeline kiểm tra số
trường bằng dấu phẩy ngoài việc dùng CSV parser; không hỗ trợ cột văn bản chứa
dấu phẩy hoặc CSV nhiều dòng. Dòng rỗng trong phần dữ liệu được coi là lỗi,
không âm thầm bỏ qua. Header đúng được loại khỏi dữ liệu.

### Schema đầu vào

- `match_id`: Long.
- `minute`, `radiant_win`, 10 hero ID và các cột đếm nguyên: Integer.
- Các chỉ số còn lại: Double; `_corrupt_record`: String phục vụ kiểm tra lỗi.
- Các trường bắt buộc: match ID, phút, nhãn và 10 hero ID.
- Chỉ số trong trận bị thiếu không làm loại trận nếu các trường bắt buộc hợp
  lệ. Lỗi cú pháp CSV/kiểu số vẫn được ghi nhận là bản ghi lỗi.

### Quy tắc hợp lệ và mã lý do

| Mã | Ý nghĩa |
|---|---|
| `MALFORMED_CSV` | CSV lỗi, sai kiểu số hoặc không đủ/đúng 48 trường |
| `INVALID_MATCH_ID` | Thiếu match ID hoặc ID không dương |
| `INVALID_MINUTE` | Phút thiếu, sai kiểu hoặc âm |
| `INVALID_LABEL` | Nhãn thiếu hoặc không phải 0/1 |
| `INVALID_HERO_ID` | Thiếu hero hoặc hero ID không dương |
| `DUPLICATE_HERO` | Hero trùng trong đội hoặc giữa hai đội |
| `HAS_INVALID_SNAPSHOT` | Trận có ít nhất một snapshot không hợp lệ |
| `CONFLICTING_SNAPSHOT_KEY` | Cùng match/phút có nhiều dòng khác nhau sau loại trùng |
| `CONFLICTING_DRAFT` | Đội hình đã sắp xếp không nhất quán giữa các snapshot |
| `CONFLICTING_LABEL` | Kết quả thắng/thua không nhất quán |
| `UNIDENTIFIABLE_MATCH` | Không thể parse match ID để gắn dòng vào một trận |

Một trận có thể có nhiều lý do. Nếu có một snapshot lỗi thì cách ly toàn bộ
trận, gồm cả các snapshot còn lại hợp lệ. Không chọn snapshot đầu tiên để che
mâu thuẫn. Việc chọn một đại diện chỉ xảy ra sau khi đã xác nhận duy nhất một
đội hình và một nhãn hợp lệ trong toàn bộ trận.

## Đầu ra

Mặc định:

```text
hdfs://localhost:9000/dota/processed/match_drafts_v1/
```

| Cột | Kiểu | Điều kiện |
|---|---|---|
| `match_id` | Long | Dương, duy nhất |
| `radiant_heroes` | Array<Integer> | 5 ID dương, tăng dần |
| `dire_heroes` | Array<Integer> | 5 ID dương, tăng dần |
| `radiant_win` | Integer | 0 hoặc 1 |

Không có hero trùng trong tổng 10 hero. `match_id` chỉ dùng đối chiếu và chia
tập sau này, không phải đặc trưng ML. Không suy diễn `patch` hoặc `start_time`
từ match ID; cần nguồn metadata bổ sung nếu muốn phân tích theo patch/thời gian.

Khi có bản ghi lỗi, lưu nguyên dòng, các giá trị đã parse, `row_reasons` và
`match_reasons` vào Parquet:

```text
/dota/processed/quarantine/match_drafts_v1/
```

Không tạo bảng quarantine rỗng khi không có lỗi.

Báo cáo mặc định:

- `evidence/step2/report.json`: metrics, null, lý do, nhãn, schema, thời gian.
- `evidence/step2/report.md`: bản báo cáo dễ đọc.
- `evidence/step2/sample_matches.json`: 10 trận mẫu theo match ID.
- `evidence/step2/rule_tests.json`: kết quả kiểm thử bằng fixture nhỏ.
- `/dota/analytics/data_quality/match_drafts_v1.json`: bản JSON trên HDFS.

Khi chọn output mới, tên cuối của output quyết định tên bảng quarantine,
JSON HDFS và thư mục báo cáo local `evidence/step2/<tên-output>`.

`identified_matches` đếm mọi match ID không null (kể cả ID không dương bị
cách ly); `unidentifiable_source_rows` đếm các dòng không parse được ID.
`invalid_source_rows` và thống kê null tính trên dữ liệu nguồn trước loại
trùng; `quarantined_unique_rows` tính sau loại trùng. Tổng số trận hợp lệ và
trận bị cách ly phải khớp số trận xác định được.

## Kiểm thử

Kiểm thử nhỏ không ghi bảng HDFS:

```bash
cd /mnt/d/BigDataTH_Project/Dota2_BigData
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=/usr/bin/python3
/home/danh/.local/bin/spark-submit --master 'local[2]' --driver-memory 1g \
  --conf spark.driver.host=127.0.0.1 \
  --conf spark.driver.bindAddress=127.0.0.1 \
  tests/test_prepare_match_drafts.py
```

Các tình huống gồm snapshot nhất quán, đổi thứ tự hero, thiếu/trùng hero,
hero ID không dương, nhãn lỗi, thay đội hình/kết quả, dòng trùng, cùng khóa
khác telemetry, thiếu chỉ số không sử dụng, không xác định match ID, CSV thừa
trường và phút sai kiểu/âm.

Kiểm chứng toàn bộ dữ liệu yêu cầu:

- Đối chiếu 1.007.768 snapshot và 90.530 trận với nguồn hiện tại.
- Dữ liệu khác tham chiếu được ghi rõ trong `reference_differences`, không
  ép số lượng bằng cách bỏ qua lỗi.
- Đọc lại Parquet: đúng bốn cột, kiểu dữ liệu, khóa duy nhất, hero và nhãn hợp lệ.
- Có đúng hai file dữ liệu Parquet, không partition theo match ID.
- Chỉ công bố PASS sau khi báo cáo đã được sao chép thành công lên HDFS.

Xem `evidence/step2/report.md` để biết số liệu thực tế của lần chạy hoàn thành.

## Kết quả thực hiện ngày 25/10/2026

| Chỉ tiêu | Kết quả |
|---|---:|
| Snapshot nguồn | 1.007.768 |
| Trận hợp lệ / match ID duy nhất | 90.530 |
| Dòng trùng hoàn toàn | 0 |
| Snapshot không hợp lệ | 0 |
| Trận mâu thuẫn / bị cách ly | 0 |
| Giá trị null trong 48 cột | 0 |
| Radiant thắng | 46.305 |
| Dire thắng | 44.225 |
| Thời gian pipeline, không gồm khởi động Spark | 114,32 giây |
| Parquet dữ liệu | 2 file Snappy, mỗi file khoảng 669,5 KiB |

Parquet đã được Spark đọc lại và kiểm tra; HDFS `fsck` trả về HEALTHY.
JSON báo cáo trên HDFS có SHA-256 trùng bản local. Kiểm thử bằng 22 dòng giả
lập đạt các tình huống nêu trên. Chạy lại vào cùng output đã được thử thực tế:
pipeline báo lỗi và không ghi đè dữ liệu; bằng chứng ở `overwrite_guard.json`.
Không tạo bảng quarantine vì dữ liệu thực tế không có lỗi theo các quy tắc này.

Lần thử đầu với cache toàn bộ bảng rộng gặp thiếu Java heap; chưa ghi Parquet.
Sau khi chuyển cache lớn sang đĩa, giảm partition đọc/batch cache, pipeline
hoàn tất với heap 1 GB. Log được giữ trong `evidence/step2` để đối chiếu.
