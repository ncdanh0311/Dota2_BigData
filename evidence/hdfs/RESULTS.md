# Kết quả triển khai HDFS Dota 2

Thực hiện ngày 24/10/2026 (Asia/Saigon). Cụm riêng: `/home/danh/dota-hdfs`.

| Hạng mục | Kết quả thực tế |
|---|---|
| NameNode / DataNode live | 1 / 3 |
| Hadoop / Java | 3.5.0 / 17 |
| Dữ liệu | Zenodo v3, 1.007.768 snapshot, 48 cột |
| Kích thước file | 223.002.035 byte |
| Block size | 67.108.864 byte (64 MiB) |
| Số block logic | 4 |
| Replication | 2; mỗi block có đúng hai replica |
| Tổng replica | 8 |
| Dung lượng theo replication (`du`) | 446.004.070 byte |
| FSCK | HEALTHY; missing/corrupt/under-replicated blocks đều bằng 0 |
| MD5 nguồn | Khớp giá trị công bố của Zenodo v3 |
| SHA-256 HDFS và nguồn | Khớp |
| Khởi động lại toàn cụm | PASS; không format lại, dữ liệu giữ nguyên |
| Dừng riêng DN3 và đọc toàn bộ file | PASS; SHA-256 vẫn khớp |
| Sau khi bật lại DN3 | 3 DataNode live, HEALTHY, đủ replica |

## Block thực tế

| Block | Kích thước byte | Vị trí replica |
|---|---:|---|
| 0 | 67.108.864 | DN2 + DN3 |
| 1 | 67.108.864 | DN1 + DN3 |
| 2 | 67.108.864 | DN1 + DN3 |
| 3 | 21.675.443 | DN1 + DN2 |

DN1: `127.0.0.1:9866`; DN2: `127.0.0.1:9966`; DN3: `127.0.0.1:10066`.
Đây là vị trí tại thời điểm kiểm tra; HDFS có thể thay đổi vị trí replica.

```text
MD5:    f1c2362395ab39d5510a0a008d1cbe26
SHA256: 61b3852b5eedbf0f6f0688564dbd178238c22001741089eb64e736ad1102d044
```

## Đường dẫn dữ liệu

```text
hdfs://localhost:9000/dota/raw/zenodo_v3/DB_03_Live_Features_ANALYSIS_v3.csv
```

Trong bài kiểm tra dừng DN3, HDFS client ghi cảnh báo `Connection refused`
với node bị dừng rồi kết nối sang replica trên DN1 và hoàn tất đọc file.
Đó là hành vi chuyển sang bản sao còn hoạt động, không phải lỗi mất dữ liệu.

Cả ba DataNode cùng một WSL; phép thử chỉ mô phỏng lỗi tiến trình.
Chưa có ảnh chụp màn hình; bằng chứng được lưu dưới dạng báo cáo văn bản/JSON.

Header có `rad_h1`–`rad_h5`, `dire_h1`–`dire_h5`, `radiant_win` nhưng không
có `patch` hoặc `start_time`. Bước Spark cần tạo một dòng mỗi trận cho bài
toán dự đoán trước trận và không sử dụng các chỉ số trong trận làm đầu vào.
