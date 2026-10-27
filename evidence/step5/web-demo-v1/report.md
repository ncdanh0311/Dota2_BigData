# Bước 5 — Nghiệm thu snapshot thật `web-demo-v1`

- Ngày kiểm chứng: 27/10/2026 (Asia/Saigon).
- Gold: `step3-20261026-v2`; model: `step4-20261027-v1`.
- 90.530 trận; 127 hero; 254 dòng theo bên; 7.996 cặp cùng đội.
- Gold so với tái tạo từ Silver và JSON đọc lại: PASS theo khóa/schema/giá trị, tolerance 1e-9.
- Fingerprint Silver của hai run khớp; bundle SHA-256 khớp manifest bước 4.
- 127 tên hero; CDN ảnh lỗi TLS: 0 PNG tải thành công, 127 placeholder local.
- 26 trường hợp API/bundle parity khớp hoàn toàn; 127 route ảnh local trả 200.
- HDFS cổng 9000 đã dừng; tiến trình web kiểm thử không import Spark.
- Chặn kết nối socket khi nạp snapshot và gọi API TestClient: PASS.
- Không thay đổi cấu hình mạng hệ điều hành.
- Browser desktop 1440×1000 và mobile 390×844: chọn thủ công 10 hero,
  khóa hero trùng, xóa/đổi đội, tìm kiếm và phân trang hoạt động.
- Demo: Radiant 49,72%, Dire 50,28%; sau đổi bên Radiant khoảng 52,11%.
- Thống kê tìm Rubick ra một hero; lọc cặp Rubick có 126 cặp;
  bật mẫu thiếu trả đủ 7.996 cặp, mặc định chỉ 5.611 cặp đủ ngưỡng.
- Xếp hạng hero win rate đủ mẫu đứng đầu Chen: 2.852 trận, 59,36%.

`export_checks.json`, `image_status.json`, `runtime_checks.json` và
`browser_checks.json` ghi chi tiết; bốn PNG là ảnh giao diện thật.
Các file `../*_tests.json` ghi fixture nhỏ, không phải kết quả dataset thật.

Mô hình có accuracy test 54,49%, baseline 51,23%, ROC-AUC 0,5589;
F1 Radiant thấp hơn baseline. Thống kê lịch sử không chứng minh phối hợp nhân quả
và dự đoán chưa đại diện meta hiện tại. Ảnh hero thật vẫn phụ thuộc CDN truy cập được
trong một lượt chuẩn bị snapshot mới; bản demo này dùng placeholder đã kiểm chứng.
