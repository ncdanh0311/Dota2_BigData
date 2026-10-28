# Tải bằng chứng từ GitHub Actions

`evidence/` là thư mục output local và được `.gitignore` bỏ qua toàn bộ. Clone
repo mới chỉ lấy mã nguồn/cấu hình/tài liệu; người xem tải báo cáo và ảnh từ
**Artifacts** của Actions. Các file local đang có được giữ nguyên.

## Cách tải

1. Mở [Actions](https://github.com/ncdanh0311/Dota2_BigData/actions), đăng nhập
   GitHub và chọn workflow/lượt chạy cần xem.
2. Mở trang Summary, cuộn xuống **Artifacts**.
3. Chọn gói bên dưới để tải ZIP. Giải nén, đọc `README.md` và `manifest.json`.
4. Mỗi manifest ghi nguồn, revision, run ID/attempt, thời gian đóng gói, kích
   thước và SHA-256 của từng file. Artifact được giữ **14 ngày**.

| Gói | Nguồn và nội dung |
|---|---|
| `bang-chung-ci-spark-web-<run>-<attempt>` | Fixture vừa chạy: normalization, Gold, ML, API/export/display, checkpoint recovery; JSON và log |
| `bang-chung-ci-airflow-<run>-<attempt>` | Fixture runner và parse DAG vừa chạy; JSON và log |
| `bang-chung-ci-repository-<run>-<attempt>` | Kiểm tra JSON/file Git/PowerShell và packaging; log |
| `bang-chung-do-an-da-ghi-nhan-<run>-<attempt>` | Bản lưu báo cáo/ảnh đồ án đã ghi nhận trên WSL và các kiểm thử trước đây; giữ nguyên nội dung gốc |

## Bằng chứng đồ án đã ghi nhận

Mở workflow [Publish recorded project evidence](https://github.com/ncdanh0311/Dota2_BigData/actions/workflows/publish-recorded-evidence.yml).
Workflow chạy khi công cụ đóng gói được cập nhật, hoặc chọn **Run workflow** để
xuất lại ZIP. Mặc định lấy thư mục evidence từ revision đã kiểm chứng trước
khi chuyển sang chính sách ignore:

```text
243d474f0df1d5e7ada1a832850676fd0b5bee8c
```

Gói này chứa báo cáo HDFS, chuẩn hóa, Gold, model metrics, Kafka/Airflow và ảnh
website/Graph/Grid/log đã ghi nhận. Source kind là `recorded_project`, trạng thái
`ARCHIVED`. Lượt Actions chỉ đọc lại revision cũ và đóng gói; không chạy lại
90.530 trận, không huấn luyện lại model và không giả lập báo cáo cũ là kết quả
runtime của CI mới. Từng báo cáo vẫn ghi đúng nguồn fixture hoặc run thật.

Code/evidence cũ vẫn truy vết được trong lịch sử Git; ở nhánh main hiện tại,
evidence không còn được theo dõi. Không rewrite hoặc xóa lịch sử để đạt việc này.

## Bằng chứng CI mới

Ba workflow kiểm thử tạo lại báo cáo trên runner rồi đóng gói. Báo cáo cũ được
dọn trước khi test. Nếu test lỗi hoặc thiếu báo cáo, gói ghi
`INCOMPLETE_OR_FAILED`, không coi là PASS. Log vẫn được upload khi có lỗi để
chẩn đoán. Các gói này không phải bằng chứng chạy cụm HDFS/Kafka thật.

Các Actions đã được cập nhật sang bản release chính thức dùng Node 24, thay
cho các action Node 20 cũ trong ảnh tham khảo. Không cần Node.js để chạy website.

## Dữ liệu và artifact lớn

Các gói evidence gồm báo cáo/ảnh/log, không gồm CSV 223 MB, Parquet nguồn,
checkpoint đầy đủ hoặc model native. Chúng vẫn nằm trong HDFS/local artifact.
Máy clone mới cần chuẩn bị dữ liệu/model theo hướng dẫn nếu muốn chạy website.

Những lần chạy WSL mới tiếp tục ghi `evidence/` local. Workflow bản lưu trên chỉ
công bố bằng chứng có ở revision được chọn; nó không đọc được ổ đĩa WSL của
người dùng từ GitHub-hosted runner.
