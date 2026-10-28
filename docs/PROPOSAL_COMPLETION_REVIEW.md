# Đối chiếu đề xuất với triển khai thực tế

Rà soát **28/10/2026**: đã đọc toàn bộ file `Pasted text.txt` người dùng gửi và
[trang ChatGPT chia sẻ](https://chatgpt.com/s/t_6adcd85ea68881919bf7a870ad7c7c5e).
Nội dung trang khớp với file. Bảng dưới đối chiếu source/evidence, không xem
tài liệu đề xuất là bằng chứng chức năng đã chạy.

**Kết luận:** đã hoàn thành MVP dự đoán trước trận theo phạm vi đã chốt cho
đồ án cá nhân; chưa hoàn thành mọi phương án/mở rộng của bản đề xuất ban đầu.

## Năm chức năng MVP

| Mục đề xuất | Hiện trạng | Bằng chứng |
|---|---|---|
| Chọn 5 hero mỗi bên | Đã có, chặn hero trùng/sai kiểu/ngoài mapping; sửa đội hình xóa dự đoán cũ | [browser](EVIDENCE_DOWNLOADS.md), `web/app.py` |
| Dự đoán xác suất | Logistic Regression, signed hero vector, bundle/CLI/API đối chiếu Spark | [metrics](EVIDENCE_DOWNLOADS.md), `ml/predict_draft.py` |
| Dashboard hero/win rate | Tổng trận, hai bên, hero, cặp cùng đội và ngưỡng mẫu | [Gold](EVIDENCE_DOWNLOADS.md) |
| Kafka + Spark Streaming | Phát lại trận lịch sử có kết quả, cập nhật thống kê replay riêng | [replay thật](EVIDENCE_DOWNLOADS.md), `streaming/consume_match_events.py` |
| Airflow điều phối | Hai DAG manual, phụ thuộc/retry/timeout, receipt và finalizer | [nghiệm thu](EVIDENCE_DOWNLOADS.md), `orchestration/dags/` |

Kafka là historical replay, không phải nguồn live từ OpenDota/Steam; không cộng
lại replay vào Gold. Chạy Airflow thủ công là lựa chọn demo đã thống nhất, không
phải thiếu chức năng vì chưa đặt lịch hằng ngày.

## Chín bước trong đề xuất gốc

Số bước gốc khác lộ trình 7 bước đã triển khai; đối chiếu theo công việc:

| Bước gốc | Kết quả thực tế | Trạng thái |
|---|---|---|
| Thu thập | Zenodo v3, 223.002.035 byte, 48 cột, 1.007.768 snapshot/90.530 trận | Đạt; nguồn không có patch/start_time |
| Chuẩn hóa | Schema cố định, kiểm tra snapshot/trận, dedup, quarantine | Đạt, 90.530 trận hợp lệ |
| HDFS Bronze/Silver | Raw CSV, Silver Parquet; 1 NameNode và 3 DataNode cùng WSL | Đạt mô phỏng một máy |
| Spark SQL | Hero/bên/cặp; Gold versioned và kiểm chứng đọc lại | Đạt |
| MLlib | LR, baseline, 3 regParam; chọn bằng validation, test độc lập | Đạt LR, chưa Random Forest |
| Lưu kết quả | Gold/model/metrics/manifest trên HDFS; JSON phục vụ web | Đạt, model ở `/dota/models` riêng |
| Kafka | Catalog/topic theo run, event ID, producer ACK | Đạt mô phỏng kết quả; chưa topic draft/result riêng |
| Streaming | Archive batch, checkpoint, dedup, JSON cập nhật web | Đạt thống kê; chưa inference mỗi sự kiện draft |
| Airflow | Raw có sẵn trên HDFS → Silver → Gold → LR → snapshot, DAG replay riêng | Đạt manual; upload raw ban đầu qua công cụ HDFS |

## Khác biệt và mục chưa làm

| Mục | Cách trình bày đúng |
|---|---|
| 70/15/15 theo thời gian | Đã đổi sang SHA-256 của `42:<match_id>`. Không có start_time; minute là phút trong trận. Không gọi hash split là temporal split hoặc đoán ngày từ match_id. |
| Random Forest so sánh | Chưa làm. MVP một mô hình đã đạt bằng LR; nếu rubric đòi hai thuật toán thì cần thêm RF. Ba regParam không phải ba thuật toán. |
| Dashboard theo patch | Chưa làm vì CSV không có patch; không suy ra patch từ metadata hero hiện tại. |
| `dota_match_drafts/results` | Chưa có hai topic/join. Sự kiện hiện tại chứa cả đội hình và kết quả đã biết. |
| 100 trận test, dự đoán streaming | Demo dùng catalog 1.000 trận Silver và tính thống kê, không phải đánh giá ML trên tập test hay streaming inference. |
| Airflow upload raw | Chưa nằm trong DAG; preflight cần raw đã tồn tại. Upload bằng `scripts/hdfs_cluster_manager.py`. |
| Gợi ý hero thay thế | Chưa làm; mở rộng. |
| Cặp hero phối hợp tốt | Có thống kê cặp cùng đội; chưa mô hình tương tác/causal synergy. Không gọi win rate cao là hiệu ứng nhân quả. |
| Dự đoán phút 10/20/30 | Chưa làm; model chỉ dùng 10 hero trước trận. |
| Docker/React/Streamlit | Là phương án công nghệ. Hiện dùng WSL native, FastAPI + HTML/CSS/JS. |
| Ảnh hero | Có 127 tên và ảnh local; snapshot thật còn 127 placeholder do CDN lỗi, chưa tải đủ portrait. |

Accuracy test **54,49%**, baseline **51,23%**, ROC-AUC **0,5589**; F1 Radiant thấp
hơn baseline. Chạy được hệ thống không đồng nghĩa dự đoán mạnh hoặc đúng meta hiện tại.

## Tham khảo workflow và gitignore của Chicago

Đã đọc tại commit `b3dc08716141db93839fb0e501072112a0e35807`:
[pipeline](https://github.com/DuyLinh0212/chicago-crime-bigdata-analysis/blob/b3dc08716141db93839fb0e501072112a0e35807/.github/workflows/pipeline.yml),
[Kafka](https://github.com/DuyLinh0212/chicago-crime-bigdata-analysis/blob/b3dc08716141db93839fb0e501072112a0e35807/.github/workflows/kafka-ingest.yml),
[gitignore](https://github.com/DuyLinh0212/chicago-crime-bigdata-analysis/blob/b3dc08716141db93839fb0e501072112a0e35807/.gitignore).

| Tham khảo | Quyết định cho Dota 2 |
|---|---|
| CI dựng Docker HDFS/Kafka/Postgres, chạy Airflow thật | Giữ CI fixture; chưa CI toàn cụm. Không thêm workflow cần hạ tầng chưa có hoặc dùng đường dẫn WSL trên GitHub runner. |
| Log/artifact cả khi lỗi | Bổ sung tee log, upload `if: always()`. |
| Run ID/attempt, retention | Tên artifact chứa run ID/attempt, giữ 14 ngày. |
| Job Summary | Bổ sung tóm tắt nêu đúng scope fixture; missing/corrupt report không được coi là PASS. |
| Không hủy pipeline dữ liệu khi push mới | Giữ cancel-in-progress cho fixture CI độc lập; không áp dụng cho job dữ liệu WSL. |
| Bỏ toàn bộ evidence | Đã áp dụng theo yêu cầu mới: evidence local bị Git bỏ qua; báo cáo/ảnh tải qua Actions Artifacts. |

Giữ source DAG/workflow/config và `data/reference/` trong Git. Bổ sung ignore
runtime `.airflow` lồng thư mục, mật khẩu generated, cache notebook và Spark event
logs. Không copy ignore đặc thù `f.txt` hoặc bật tất cả người dùng thành admin.

Chính sách evidence đã đổi theo yêu cầu tiếp theo của người dùng: không còn
commit báo cáo/ảnh trên main. Workflow bản lưu đọc revision gốc để xuất gói
download, phân biệt với fixture mới; xem [hướng dẫn tải](EVIDENCE_DOWNLOADS.md).

## Nên bổ sung tiếp

1. Chốt phạm vi trong báo cáo; chuẩn bị video/kịch bản demo preflight lỗi, Kafka
   retry và web đọc snapshot local.
2. Thêm RF nếu môn học đòi so sánh nhiều thuật toán hoặc muốn bám toàn bộ phần ML.
3. Hai topic và streaming inference là mở rộng mới, không phải đổi tên file.
4. CI Docker toàn cụm là tùy chọn sau khi có runtime portable và giới hạn dataset
   rõ ràng; chưa cần cho MVP cá nhân đã chạy thật trên WSL.

Không cần thêm database/tài khoản/Kubernetes/dự đoán trong trận chỉ để hoàn thành
MVP. Các phần mới ở lượt rà soát này là tên file, CI, ignore và tài liệu đối chiếu.
