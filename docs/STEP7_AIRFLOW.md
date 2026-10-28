# Bước 7 — Điều phối bằng Apache Airflow

Airflow điều phối các job bước 2–6 đã có. Hai DAG chạy thủ công, không tự chạy
theo lịch. Mỗi lần chạy giữ riêng lineage, log, đầu ra và báo cáo. Dữ liệu replay
vẫn là dữ liệu lịch sử được phát lại, không phải trận Dota 2 trực tiếp.

## 1. Môi trường

| Thành phần | Cấu hình |
|---|---|
| Airflow | 3.3.2, Python 3.14, venv riêng trong WSL Ubuntu |
| Cài đặt | `/home/danh/dota-airflow/venv` |
| Metadata, log và tài khoản local | `/home/danh/dota-airflow/runtime` |
| Executor / database | LocalExecutor / SQLite WAL, demo trên một máy |
| UI | `http://localhost:8080` |
| Giới hạn | parallelism 1, một task/run mỗi DAG, pool `dota_spark` một slot |
| API / DAG processor | một API worker, một tiến trình parse |
| Spark | Python hệ thống, Java 17, `local[2]`, driver 1 GiB, shuffle 8 |

Cấu hình ở `configs/airflow-wsl.json`. Cài theo constraints chính thức cho
Airflow 3.3.2/Python 3.14; pip trong venv được pin 26.2.2. Không cài Airflow vào
môi trường Spark, web hoặc Kafka producer. SQLite WAL đặt trên filesystem Linux,
không đặt database Airflow trên ổ `/mnt/d`.

## 2. Cài và bật/tắt

Từ PowerShell:

```powershell
cd D:\BigDataTH_Project\Dota2_BigData
.\install-airflow.ps1
.\start-airflow.ps1
```

Mở `http://localhost:8080`. Tài khoản là `admin`; lấy mật khẩu từ file riêng
trong WSL, không đưa file này lên Git hoặc chụp vào evidence:

```powershell
wsl -d Ubuntu -u danh -- cat /home/danh/dota-airflow/runtime/simple_auth_manager_passwords.json.generated
```

Khi không còn task chạy:

```powershell
.\stop-airflow.ps1
```

Start không reset database hoặc lịch sử; stop chỉ gửi tín hiệu tới process group
do script này tạo. Nếu còn task running/queued, stop từ chối để người chạy chờ DAG
hoàn tất. Log launcher nằm riêng tại `/home/danh/dota-airflow/launcher.log`, có thể
chứa thông tin đăng nhập do Airflow in ra, nên không đưa vào evidence.

## 3. DAG batch

```text
preflight → normalize_silver → analyze_gold → train_lr → export_snapshot
     └──────── mọi kết quả bắt buộc ───────────→ verify_and_report
```

1. Bật HDFS bằng `.\start-hadoop.ps1`.
2. Trong UI, mở `dota_batch_pipeline`, bỏ pause nếu cần và chọn Trigger.
3. Giữ mặc định `raw_input` là CSV Zenodo v3 và `assets_snapshot_id=web-demo-v1`.
4. Xem từng task, log và kết quả. Chỉ run có `verify_and_report` thành công mới
   được coi là hoàn tất.

DAG tạo Silver mới từ toàn bộ CSV, phân tích Gold mới, huấn luyện LR mới và
export snapshot mới. Không ghi đè Silver v1, Gold/model cũ hoặc snapshot đang dùng.
Export dùng lại metadata/ảnh từ snapshot hoàn tất được chỉ định, kiểm tra checksum
và đủ hero; không cần tải lại CDN. Snapshot nguồn hiện có 127 ảnh thay thế local,
không có ảnh portrait tải thành công. Việc tái sử dụng không thay đổi đặc trưng ML.

Run ID Airflow được hash thành `af-<16 ký tự>`. Mỗi attempt có ID riêng gồm stage,
số lần thử và hậu tố ngẫu nhiên. Khi Silver/Gold/model/export phải retry, nó ghi
namespace mới và downstream chỉ nhận receipt của attempt thành công.

## 4. DAG replay và demo retry

```text
preflight → prepare_replay → produce_events → consume_available_now
     └──────── mọi kết quả bắt buộc ───────→ verify_and_report
```

```powershell
.\start-hadoop.ps1
.\start-kafka.ps1
```

Trigger `dota_replay_demo` với:

| Param | Mặc định | Ý nghĩa |
|---|---|---|
| `silver_input` | Silver v1 trên HDFS | Nguồn catalog |
| `limit` | 1000 | Số trận lịch sử phát lại |
| `rate` | 5 | Số sự kiện/giây |
| `demo_fail_once` | false | Bật để demo lỗi có chủ đích sau ACK lần đầu |

Khi `demo_fail_once=true`, producer lần 1 ACK đủ 1.000 rồi task chủ động lỗi.
Airflow đợi 30 giây và retry cùng catalog/topic. Sau lần 2, consumer đọc hết
Kafka bằng `availableNow`, dùng cùng checkpoint khi retry. Kết quả mong đợi là
2.000 records, 1.000 trận duy nhất và 1.000 records trùng; wins/picks chỉ đếm mỗi
trận một lần. Đây là demo xử lý gửi lại theo mô hình at-least-once; không gọi nó
là Kafka end-to-end exactly-once. Không cộng replay vào Gold lịch sử.

Mặc định mỗi task retry tối đa một lần, trừ finalizer. Batch timeout 60 phút,
replay 30 phút; mỗi Spark task có timeout riêng. Khi timeout/cancel, adapter dừng
process group của job con do chính task tạo, giữ output dở để chẩn đoán.

## 5. Theo dõi và khôi phục

- XCom chỉ có đường dẫn receipt và SHA-256. Receipt ghi attempt, số lần thử,
  đầu ra và receipt nguồn; không chứa bảng dữ liệu hoặc model.
- Artifact điều phối: `artifacts/airflow/<dag_id>/<execution_id>/` (Git bỏ qua).
- Báo cáo nhỏ: `evidence/step7/<execution_id>/report.json` và `report.md`.
- Evidence chuyên môn vẫn ở `evidence/step2`, `step3`, `step4`, `step5`, `step6`
  theo ID đầu ra do Airflow tạo.
- `_COMPLETED` ở thư mục artifact điều phối chỉ xuất hiện khi kiểm chứng cuối
  cùng đạt. Spark `_SUCCESS` hoặc task upstream xanh chưa đủ để xác nhận run.
- Finalizer dùng `all_done` để ghi nhận lỗi nhưng bắt buộc đủ receipt hợp lệ;
  thiếu upstream thì nó fail, không làm run lỗi thành màu xanh.

Nếu preflight fail, bật đúng HDFS/Kafka rồi tạo run mới hoặc clear task lỗi và
downstream. Nếu clear `prepare_replay`, catalog/topic mới được tạo; nếu chỉ retry
producer/consumer thì tiếp tục catalog/checkpoint cũ. Không tự xóa output lỗi,
không format HDFS hoặc Kafka để sửa run.

Có thể kiểm tra bằng REST helper, chạy với Python hệ thống WSL. Helper đọc mật
khẩu riêng và không in token; ví dụ từ thư mục repo trong WSL:

```bash
python3 orchestration/airflow_run_control.py trigger --dag-id dota_replay_demo \
  --run-id demo-manual-001 --conf '{"limit":1000,"rate":5,"demo_fail_once":true}'
python3 orchestration/airflow_run_control.py tasks --dag-id dota_replay_demo --run-id demo-manual-001
python3 orchestration/airflow_run_control.py wait --dag-id dota_replay_demo --run-id demo-manual-001
```

Helper không tạo lịch định kỳ. Khi `wait` kết thúc, nó lưu trạng thái run/task
để đối chiếu trong evidence. Với bài kiểm tra cố ý lỗi, thêm `--expect failed`.

## 6. Kiểm thử và bằng chứng

`tests/test_workflow_job_runner.py` dùng fixture local, kiểm tra receipt/checksum,
namespace retry, producer retry cùng catalog, dừng job con khi timeout và cache
ảnh không gọi mạng. `tests/test_airflow_dags.py` parse bằng SDK pin, kiểm tra DAG,
manual schedule, pool, concurrency và finalizer; cấm launch job/kết nối mạng khi
parse. Không import PySpark hoặc Kafka client trong tiến trình parse DAG.

Lệnh kiểm thử chung `scripts/run-tests.ps1` có thêm kiểm tra runner và DAG (DAG
cần setup Airflow trước). Workflow `.github/workflows/airflow-dag-tests.yml` có môi trường
Airflow riêng, không cần HDFS,
Kafka hoặc dữ liệu thật. Workflow chạy các kiểm thử fixture khi push lên GitHub;
xem kết quả ở [GitHub Actions](https://github.com/ncdanh0311/Dota2_BigData/actions).
Dataset, model và snapshot lớn tiếp tục nằm local/HDFS, không đưa lên Git.
Workflow Spark/web ở `spark-web-tests.yml`; `repository-quality.yml` kiểm tra JSON, quy tắc file
Git và cú pháp PowerShell. Các workflow có concurrency để tránh chạy chồng các
lượt cũ của cùng workflow/ref.

Kết quả chạy thật và ảnh Graph/Grid được ghi trong `evidence/step7/RESULTS.md`.
Không trộn kiểm thử fixture với các run 90.530 trận hoặc replay Kafka thật.

Hai lệnh kiểm chứng các run đã chạy, từ WSL tại thư mục repo:

```bash
python3 orchestration/verify_pipeline_runs.py --batch-run-id step7-batch-real-v1 \
  --replay-run-id step7-replay-retry-v1 --failure-run-id step7-preflight-offline-v1
.venv-web/bin/python scripts/verify_orchestrated_website.py --batch-run-id step7-batch-real-v1 \
  --replay-run-id step7-replay-retry-v1
```

Lệnh đầu cần Airflow đang chạy để đối chiếu API và lịch sử; lệnh web đọc artifact
local, chặn socket trong kiểm thử ASGI. Có thể thêm `--live-port 8001` khi server
đang chạy và HDFS/Kafka đã tắt để kiểm tra thêm HTTP thật.

## 7. Giới hạn

Đây là cấu hình demo một máy cho đồ án cá nhân, chưa phải cluster Airflow nhiều
worker. Airflow điều phối job; Spark vẫn chạy local[2], ba DataNode thuộc phần
lưu trữ HDFS. Dự đoán giữ nguyên giới hạn dữ liệu lịch sử và metric bước 4.
