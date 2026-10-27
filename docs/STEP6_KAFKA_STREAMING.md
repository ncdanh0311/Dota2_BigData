# Bước 6 — Kafka replay và Spark Structured Streaming

Đã triển khai và nghiệm thu local ngày 27/10/2026. Workflow kiểm thử fixture
được tích hợp vào GitHub Actions; kết quả replay thật được lưu riêng trong evidence.

## Kiến trúc và giới hạn

```text
Silver HDFS → catalog 1.000 trận bất biến → Python producer
    → Kafka 2 partition → Spark Structured Streaming / foreachBatch
    → HDFS archive batch đã commit → Spark SQL thống kê riêng
    → JSON local thay nguyên tử → FastAPI → tab Mô phỏng Kafka
```

Đây là replay dữ liệu lịch sử, không phải trận mới thu thập từ Dota/OpenDota.
Thời điểm phát `emitted_at` là UTC của producer, không phải thời gian trận.
Không cộng replay vào Gold, không huấn luyện lại model và không thêm feature ML.

Kafka 4.1.2 chạy native WSL KRaft, một tiến trình kiêm broker/controller.
Java 17, heap Kafka 256–512 MiB; dữ liệu/config/PID riêng tại `/home/danh/dota-kafka`.
Broker `127.0.0.1:9092`, controller `127.0.0.1:9093`, replication 1, retention 7 ngày.
Một broker không chịu được mất broker/disk; ba DataNode vẫn chỉ là mô phỏng cùng WSL.

Spark 4.2.0 dùng `local[2]`, driver 1g, shuffle 8, timezone UTC,
connector `spark-sql-kafka-0-10_2.13:4.2.0`, trigger 10 giây và tối đa
500 Kafka records/batch. Tái sử dụng JAR Hadoop/Scala/compression có sẵn trong PySpark;
Maven resolution dùng cấu hình Ivy của repo. Producer dùng `kafka-python==3.0.12`
trong `.venv-streaming`, web vẫn dùng `.venv-web`.

Thiết kế tính lại trên archive mỗi batch phù hợp phiên demo hữu hạn 1.000 trận.
Không tuyên bố đây là triển khai tối ưu cho luồng không giới hạn.

## Cài và chuẩn bị phiên

```powershell
cd D:\BigDataTH_Project\Dota2_BigData
.\setup-step6.ps1
.\start-hadoop.ps1
.\start-kafka.ps1

# Chọn run ID mới; replay-demo-v1 đã có trên máy hiện tại.
.\prepare-step6.ps1 -RunId replay-demo-v2 -Limit 1000
```

Setup tải archive Kafka chính thức qua Windows từ `dlcdn.apache.org`, kiểm SHA-512
trong WSL trước giải nén, cài producer và kiểm tra connector. Nếu đường tải Apache
chậm, có thể dùng `setup-step6.ps1 -ParallelDownload`: tải range có giới hạn song song,
ghép theo đúng thứ tự rồi vẫn kiểm toàn bộ SHA-512. Cache nằm trong `artifacts/cache`.
Lượt setup lặp lại đã kiểm chứng giữ cluster ID/storage; start không tự format.

Prepare kiểm tra Silver, lấy 1.000 match ID nhỏ nhất theo thứ tự tăng dần,
sắp xếp hero, lưu catalog Parquet HDFS + JSONL local. Manifest chứa fingerprint
Silver, SHA-256 catalog, số trận, nhãn tham chiếu và topic ID thực tế.
Mỗi run có topic riêng `dota.matches.replay.<run_id>`, hai partition.
Không ghi đè run, không reset topic/checkpoint; muốn replay độc lập thì tạo ID mới.

## Chạy và dừng

Mở ba PowerShell tại thư mục repo:

```powershell
# Terminal 1: consumer liên tục, Ctrl+C để dừng.
.\run-step6.ps1 -RunId replay-demo-v2
```

```powershell
# Terminal 2: mặc định 5 trận/giây, 1.000 trận khoảng vài phút.
.\replay-matches.ps1 -RunId replay-demo-v2 -Rate 5
```

```powershell
# Terminal 3: website; -ReplayRunId là tùy chọn bổ sung của bước 6.
.\start-web.ps1 -SnapshotId web-demo-v1 -ReplayRunId replay-demo-v2
```

Mở [http://localhost:8000/#replay](http://localhost:8000/#replay).
Tab mô phỏng tải lại mỗi 3 giây khi trang/tab đang hiển thị. Tần suất đọc giao diện
không có nghĩa Spark hoàn tất batch sau đúng 3 hoặc 10 giây.
Khi không có batch mới, hiển thị tuổi dữ liệu; không tự kết luận broker hỏng.
Win rate mặc định chỉ xếp hạng dòng đủ mẫu 100 trận; checkbox cho xem mẫu chưa đủ.

Trong phiên đã chuẩn bị, có thể chạy lại producer để kiểm tra duplicate.
`-Rate 50` chỉ dùng trong lượt nghiệm thu phát lại; mặc định vẫn là 5.
Consumer có `-StopAfterSeconds` cho phép thử dừng và `-AvailableNow` để vét backlog
hiện có rồi tự dừng. Nếu producer vẫn đang gửi, dùng consumer liên tục để nhận phần đến sau.

Dừng consumer trước bằng Ctrl+C; sau đó:

```powershell
.\stop-kafka.ps1
.\stop-hadoop.ps1
```

Để tiếp tục: bật lại HDFS/Kafka, chạy consumer với **cùng RunId**, giữ checkpoint.
Không chạy hai consumer cùng run; filesystem lock tự nhả khi tiến trình kết thúc.
Mất checkpoint nhưng còn archive sẽ bị từ chối, không tự phát lại với trạng thái mới.

## Quy tắc xử lý và dữ liệu

Event gồm phiên bản 1, run ID, `event_id=<run_id>:<match_id>`, bốn trường Silver,
`emitted_at` và `source_mode=historical_replay`; Kafka key phải bằng event ID.
Producer bật idempotence + acks all, đợi ACK, flush trước báo PASS. Chạy lại producer
cố ý tái sử dụng event ID; chống đếm trùng xuyên lần khởi động là trách nhiệm consumer.

Trong mỗi batch, tối đa 500 records được lấy về driver để kiểm JSON/type nghiêm ngặt
và đối chiếu catalog bằng Python chuẩn, không dùng Python UDF. Bước kiểm tra nhỏ này
được chặn kích thước rõ ràng; archive, dedup và tổng hợp kết quả vẫn dùng Spark DataFrame/SQL.
Thiếu/trùng hero, boolean/float thay integer, nhãn lỗi, key/run/time lỗi, trận ngoài
catalog hoặc thay đổi đội hình/kết quả đều vào quarantine kèm JSON gốc và reason.
Đổi thứ tự hero trong một đội vẫn hợp lệ.

```text
/dota/streaming/replay_v1/<run_id>/
├── input/                  catalog Parquet, manifest, _COMPLETED
├── consumer_identity.json  phiên bản logic, topic ID, SHA catalog
├── checkpoint/             offset và commit log Structured Streaming
├── batches/<batch_id>/     valid/, quarantine/, manifest, _COMMITTED
├── statistics/<batch_id>/  hero_stats/, match_overview/, snapshot.json, _COMMITTED
└── _staging/               staging hoặc phần dở giữ để chẩn đoán
```

Batch viết staging, đọc lại theo khóa/schema/giá trị, rồi rename nguyên tử.
Chỉ batch `_COMMITTED` được tính. Retry cùng batch ID bắt buộc fingerprint giống nhau;
không ghi lần hai nhưng vẫn hoàn tất bước publish nếu lần trước bị ngắt.
Thống kê lấy match ID duy nhất từ mọi batch đã commit; nội dung đã được catalog
đảm bảo nhất quán nên không tùy tiện chọn giữa các đội hình mâu thuẫn.

Với R records, V hợp lệ và N trận duy nhất: quarantine = R−V, duplicate = V−N.
Tổng hero picks = 10N, wins = 5N; Radiant dùng nhãn, Dire dùng 1−nhãn.
Pick rate = picks/N, win rate = wins/picks, coverage = N/số trận catalog.
Các tỷ lệ là Double 0–1; mẫu số 0 trả null. Coverage chỉ đo phiên replay.

Serving JSON local: `artifacts/streaming/<run_id>/latest.json`, có checksum payload,
ghi file tạm + fsync + os.replace. Web chỉ đọc JSON, không gọi Kafka/HDFS/Spark.
`GET /api/streaming` trả trạng thái not_configured/waiting/ready/stale/complete/read_error.
Checksum lỗi giữ bản tốt gần nhất với trạng thái lỗi, không giả lập dữ liệu mới.
Khi consumer khởi động lại, publish lại snapshot HDFS trước khi chờ dữ liệu mới.

## Kết quả nghiệm thu thật

Run `replay-demo-v1`, source 90.530 trận; catalog SHA-256
`335e4c8a9c6b1b2ed672147c221ef1e79ef0ae11f5e8ee4d3170f9821c8842a7`.

| Kiểm chứng | Kết quả |
|---|---:|
| Lượt đầu, 5 trận/giây | 1.000 ACK trong 204,40 giây |
| Hai partition lượt đầu | 515 và 485 records |
| Consumer dừng giữa phiên | Snapshot 391 trận đã commit |
| Khôi phục cùng checkpoint | Đủ 1.000 trận, không trùng |
| Radiant / Dire thắng | 454 / 546 |
| Hero picks / wins | 10.000 / 5.000 |
| Lượt phát lại 50/giây | Thêm 1.000 ACK trong 24,78 giây |
| Cuối cùng | 2.000 records, 1.000 trận duy nhất, 1.000 trùng, 0 cách ly |

Đối chiếu từng hero bằng Counter Python độc lập: PASS. Broker đã stop/start,
consumer vẫn nhận đúng topic ID và checkpoint; offset lượt sau nối tiếp lượt trước.
Các batch thật có lúc vượt trigger 10 giây, gồm batch backlog khoảng 25 giây
theo log; không khẳng định độ trễ cố định. JSON hiển thị thời gian tổng hợp trước
kiểm chứng output; `last_query_progress.json` chứa durationMs toàn trigger của Spark.

Ở lần đo có HDFS/Kafka/Spark/web đồng thời: WSL 5.639 GiB RAM,
còn khoảng 2,399 GiB khả dụng, chưa dùng swap. Các RSS theo tiến trình nằm trong evidence.

## Kiểm thử, bằng chứng và trạng thái Git

`tests/test_streaming.py` dùng Structured Streaming file source + checkpoint thật,
thử crash sau archive commit, restart, duplicate, quarantine và restore JSON.
`tests/test_streaming_api.py` kiểm trạng thái API, checksum/JSON lỗi và giữ nguyên
Gold/prediction. Node unit check hiện có vẫn đạt; website không cần build frontend.

```powershell
# Kiểm chứng snapshot thật theo catalog, không cần Spark.
wsl -d Ubuntu -u danh --cd /mnt/d/BigDataTH_Project/Dota2_BigData `
  --exec .venv-streaming/bin/python scripts/verify-step6.py `
  --run-id replay-demo-v1 --expected-records 2000

# Toàn bộ fixture; không cần Kafka/HDFS cho fixtures.
.\scripts\run-tests.ps1
```

Bằng chứng nhỏ tại `evidence/step6/`: rule/API tests và thư mục run gồm báo cáo
producer, kiểm chứng độc lập, progress, RAM và ảnh browser. Runtime Kafka, archives,
Parquet, checkpoints và catalog lớn ở WSL/HDFS hoặc artifacts, được Git bỏ qua.

Mã nguồn được chia thành các commit riêng cho hạ tầng Kafka, pipeline replay,
website và tài liệu/CI. Xem kết quả CI theo commit trong tab Actions của repo.
Các cờ Git/CI trong evidence phản ánh thời điểm thu bằng chứng trước khi công bố;
không sửa lại các số liệu nghiệm thu lịch sử khi tạo commit hoặc push.
