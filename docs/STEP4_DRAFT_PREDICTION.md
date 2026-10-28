# Bước 4 — Dự đoán kết quả từ đội hình bằng Spark ML

> Các đường dẫn evidence là output local bị Git bỏ qua. Tải báo cáo/ảnh từ [Actions Artifacts](EVIDENCE_DOWNLOADS.md).

## Chạy huấn luyện

```powershell
cd D:\BigDataTH_Project\Dota2_BigData
.\commands\start-hadoop.ps1
.\commands\train-draft-model.ps1
```

Mặc định tạo run ID UTC ngẫu nhiên. Có thể đặt rõ:

```powershell
.\commands\train-draft-model.ps1 -RunId demo-ml-01 -Seed 42
```

Tham số: `-InputPath`, `-OutputRoot`, `-RunId`, `-Seed`; CLI Python tương ứng
`--input`, `--output-root`, `--run-id`, `--seed`. Không ghi đè run HDFS, evidence
hoặc bundle local đã có. Muốn chạy lại dùng ID mới.

Môi trường: Spark 4.2.0, Java 17, `local[2]`, heap 1 GB, shuffle partitions 8.
Requirements khai báo `pyspark[ml]==4.2.0` để CI có phụ thuộc NumPy của Spark
ML; môi trường WSL hiện tại đã có sẵn. Không dùng scikit-learn hoặc Pandas
để train. Dữ liệu gốc, Silver và Gold không bị sửa.

## Chia tập và đặc trưng

Đầu vào duy nhất là `/dota/processed/match_drafts_v1/`, bốn cột đội hình và
kết quả. Dùng lại kiểm tra Silver: đúng kiểu, khóa duy nhất, hero ID dương,
hai đội đủ năm hero và không trùng, nhãn 0/1, input không rỗng.

```text
bucket = int(sha256(str(seed) + ':' + str(match_id))[:8], 16) % 10000
bucket < 7000 → train
bucket < 8500 → validation
còn lại       → test
```

Các tập không giao nhau, cộng lại bằng toàn bộ input và đều phải có cả hai
nhãn. Đây là hash holdout, không phải đánh giá theo thời gian hoặc patch.

Chỉ lấy danh sách hero từ train, sắp ID tăng dần và ánh xạ sang vị trí vector:
`+1` Radiant, `-1` Dire, `0` vắng mặt. Spark expressions và VectorAssembler
tạo feature; không có Python UDF. Nhãn cast Double. Mỗi vector có đúng mười
phần tử khác 0. Hero holdout không có trong mapping gây lỗi, không bị bỏ qua.
Không dùng match ID, split bucket, win rate Gold, gold, kills, duration hay
item làm feature. Chỉ cột `features` được estimator sử dụng.

## Huấn luyện, chọn mô hình và metric

Baseline dùng tỷ lệ Radiant thắng của **train**, giữ cố định khi đánh giá
validation/test; dự đoán bên phổ biến hơn, hòa thì Dire. Xác suất baseline
không phải 0/1 để có thể tính log loss hữu hạn.

Chạy tuần tự Logistic Regression với `regParam` 0, 0.01, 0.1; L2, tối đa 100
vòng, tol 1e-6, có intercept, không standardize, threshold 0.5. Mỗi model chỉ
fit trên train. Chọn theo validation log loss thấp nhất; hòa trong 1e-12 thì
ưu tiên ROC-AUC cao hơn, rồi regParam lớn hơn. Không refit train+validation.

Chỉ sau khi chốt mới tính test metrics: accuracy, precision/recall/F1 lớp
Radiant, exact ROC-AUC (`numBins=0`), log loss clip p vào [1e-15, 1-1e-15],
và TN/FP/FN/TP. F1 ở đây là binary F1 của lớp 1, không phải weighted F1.
Ghi số vòng lặp, objective history và trạng thái nếu chạm maxIter; không tự
khẳng định đã hội tụ. Báo cáo trung thực nếu yếu hơn baseline.

## Artifact và kiểm chứng

```text
/dota/models/draft_lr_v1/<run_id>/
├── split_assignments/       # match_id, bucket, split
├── spark_model/             # native Spark model
├── model_bundle.json        # mapping, coefficients, intercept và class contract
├── validation_metrics.json
├── test_predictions/        # match_id, label, p_radiant, prediction
├── metrics.json
├── manifest.json
├── prediction_examples.json
├── portable_parity.json
├── report.md
└── _COMPLETED
```

Job lưu và đọc lại split assignments, native model và test prediction
Parquet. Sau đó xuất JSON bundle từ native model đã load, đọc bundle lại và
đối chiếu xác suất/nhãn trên **mọi trận test**, sai số tối đa 1e-9. Đây là
kiểm tra serialization parity, không phải một vòng tuning test khác.

Manifest ghi source files/HDFS checksum, fingerprint, split rule/counts,
vocabulary, tham số, Git commit/dirty flag, SHA-256 từng module logic, Spark
version, thời gian và giới hạn. Bundle SHA-256 được ghi để đối chiếu.

Run dùng `_RUNNING` khi tạo; chỉ chuyển thành `_COMPLETED` sau các bước kiểm
chứng và copy báo cáo thành công. Run lỗi không được tiêu thụ; dữ liệu trung
gian được giữ, có `failure.json` local và `_FAILED.json` khi có thể ghi HDFS.

Model native và prediction Parquet không lên Git. Bundle local nằm trong
`artifacts/models/<run_id>/model_bundle.json` (được Git bỏ qua). Báo cáo nhỏ và
mười ví dụ dự đoán nằm trong `evidence/step4/<run_id>/`.

## Dự đoán không cần Spark/HDFS

```powershell
.\commands\predict-draft.ps1 -RunId <run-id> `
  -Radiant "7,38,48,85,106" `
  -Dire "11,86,123,128,136"
```

Lệnh chỉ cần Python chuẩn trong WSL và bundle local. Có thể dừng Hadoop sau
khi train; không tạo Spark session/JVM để dự đoán. Khi clone repo sang máy
khác cần train lại hoặc copy bundle đúng run từ HDFS vào artifacts local.

Python CLI trực tiếp:

```bash
python3 -m ml.predict_draft \
  --bundle artifacts/models/<run-id>/model_bundle.json \
  --radiant 7,38,48,85,106 --dire 11,86,123,128,136
```

Logit = intercept + tổng hệ số Radiant - tổng hệ số Dire; dùng sigmoid ổn
định. Output JSON có run ID, hai đội, p_radiant, p_dire và predicted_side.
Radiant khi p > 0.5, còn lại Dire, khớp Spark tại điểm hòa. Tổng hai p bằng 1.
Đổi hai đội đổi dấu feature nhưng không bắt buộc p đối xứng vì có intercept.

Thiếu/trùng hero, ID không dương, boolean hoặc hero ngoài mapping bị từ chối.
Không đổi tên hero hoặc suy diễn ID từ văn bản. Tên hero sẽ là lớp UI sau này.

## Kiểm thử và giới hạn

`scripts/run-tests.ps1` chạy normalization, analytics và ba nhóm ML. CI dùng
fixture nhỏ, local filesystem và kiểm tra CLI bằng `python -S` để chứng minh
không cần site packages. CI không train toàn bộ dataset hoặc khởi tạo HDFS.

Các bằng chứng: feature_tests.json (hash/mapping/vector), training_tests.json
(metrics tính tay, selection và native reload), inference_tests.json (JSON
parity, threshold, extreme logits và input lỗi). Accuracy 100% trong fixture
nhân tạo không phải kết quả test dataset thật; xem report của run thật.

Mô hình chỉ dùng đội hình và dữ liệu pro lịch sử. Không có skill, team,
timestamp hay patch nên không đại diện cho mọi trận xếp hạng/meta hiện tại.
Xác suất là ước lượng của model, chưa qua bước calibration riêng.

## Kết quả thực tế ngày 27/10/2026

Run đã công bố: `step4-20261027-v1`. Train/validation/test lần lượt có
63.148 / 13.618 / 13.764 trận; mapping fit từ train có 127 hero. Không có
hero holdout ngoài mapping. Thời gian xử lý khoảng 57,89 giây, không gồm
khởi động Spark. Ba cấu hình dừng trước maxIter (10 / 16 / 22 vòng).

Chọn `regParam=0.0` vì validation log loss thấp nhất (0,687358), dù cấu hình
0,01 có validation accuracy cao hơn. Việc chọn không dựa vào test.

| Metric trên test | Baseline | Logistic Regression |
|---|---:|---:|
| Accuracy | 51,23% | 54,49% |
| Precision lớp Radiant | 51,23% | 55,08% |
| Recall lớp Radiant | 100,00% | 60,53% |
| F1 lớp Radiant | 0,6775 | 0,5768 |
| ROC-AUC | 0,5000 | 0,5589 |
| Log loss | 0,692868 | 0,687606 |

Accuracy tăng 3,26 điểm phần trăm và log loss giảm khoảng 0,005262. F1 của
lớp Radiant thấp hơn baseline luôn chọn Radiant (baseline có recall 1).
Không kết luận tốt hơn trên mọi metric hoặc dự đoán chính xác meta hiện tại.

Native model và JSON bundle khớp trên toàn bộ 13.764 trận test: sai số xác
suất lớn nhất 2,22e-16; mọi nhãn dự đoán giống nhau. Đã kiểm tra chống ghi đè
run và từ chối hero ngoài mapping; HDFS/local bundle và manifest có cùng hash.

Demo thực tế:

```powershell
.\commands\predict-draft.ps1 -RunId step4-20261027-v1 `
  -Radiant "7,38,48,85,106" -Dire "11,86,123,128,136"
```

Ví dụ trả về Radiant khoảng 49,72%, Dire khoảng 50,28%; đây là một dự đoán
riêng, không đại diện cho accuracy của mô hình.

Xem [báo cáo đầy đủ](EVIDENCE_DOWNLOADS.md),
[metric JSON](EVIDENCE_DOWNLOADS.md),
[parity trên test](EVIDENCE_DOWNLOADS.md) và
[kiểm chứng end-to-end](EVIDENCE_DOWNLOADS.md).
