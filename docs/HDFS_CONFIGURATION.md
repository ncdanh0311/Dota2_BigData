# HDFS cho đồ án Dota 2

## Cấu hình

| Thành phần | Giá trị |
|---|---|
| Môi trường | Ubuntu WSL2, user `danh` |
| Hadoop / Java | Hadoop 3.5.0 / OpenJDK 17 |
| Hadoop cài sẵn | `/usr/local/hadoop` |
| Cụm riêng của đồ án | `/home/danh/dota-hdfs` |
| NameNode | 1 |
| DataNode | 3 tiến trình trên cùng WSL |
| Block size | 67.108.864 byte = 64 MiB |
| Replication | 2 |
| Heap tối đa | NameNode 512 MiB; mỗi DataNode 256 MiB |
| NameNode RPC | `hdfs://localhost:9000` |
| Giao diện NameNode | http://localhost:9870 |

Cụm dùng bộ cấu hình, storage, PID và log riêng cho từng node. Không sửa
`/usr/local/hadoop/etc/hadoop` hoặc sử dụng storage của bài thực hành khác.
Không chạy đồng thời cụm khác sử dụng các cổng dưới đây.

| Node | Truyền block | HTTP | IPC |
|---|---|---|---|
| DN1 | 9866 | 9864 | 9867 |
| DN2 | 9966 | 9964 | 9967 |
| DN3 | 10066 | 10064 | 10067 |

Ba DataNode là ba tiến trình, không phải ba máy vật lý. Replication bảo vệ
trước việc dừng một tiến trình DataNode; không bảo vệ khi máy/ổ đĩa/WSL hỏng.

## Dataset và thư mục

- Nguồn: https://zenodo.org/records/23166182 (v3).
- File: `DB_03_Live_Features_ANALYSIS_v3.csv`.
- Kích thước công bố: khoảng 223 MB; script kiểm tra checksum trước khi nạp.
- MD5 công bố: `f1c2362395ab39d5510a0a008d1cbe26`.
- File gốc trong WSL: `/home/danh/dota-hdfs/source/`.
- Chi tiết kích thước, header, số dòng và SHA-256: `evidence/hdfs/source_manifest.json`.

```text
/dota/
├── raw/zenodo_v3/DB_03_Live_Features_ANALYSIS_v3.csv
├── processed/
└── analytics/
```

Raw được giữ nguyên, không loại snapshot hoặc làm sạch ở bước này.
Một dòng là một snapshot, không phải một trận độc lập. File có các cột
`rad_h1` đến `rad_h5`, `dire_h1` đến `dire_h5` và `radiant_win`.
Không có `patch` hoặc thời điểm bắt đầu trận trong header CSV này; cần bổ sung
metadata trước khi chia train/test theo thời gian hoặc thống kê theo patch.

HDFS chia file thành block tự động. Người dùng không tự chia file cho ba node.
Với replication 2, mỗi block có hai bản sao trên hai DataNode khác nhau;
không đảm bảo dung lượng chia đều giữa ba node.

## Vận hành bằng script

### Bật/tắt từ PowerShell trên Windows

Hai file ở thư mục gốc dự án gọi Ubuntu WSL bằng user `danh` và tự tìm
đường dẫn script Python dựa trên vị trí file PowerShell:

```powershell
cd D:\BigDataTH_Project\Dota2_BigData
.\commands\start-hadoop.ps1
.\commands\stop-hadoop.ps1
```

Nếu PowerShell chặn thực thi script, có thể chạy riêng từng lệnh bằng:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File D:\BigDataTH_Project\Dota2_BigData\start-hadoop.ps1
powershell.exe -NoProfile -ExecutionPolicy Bypass -File D:\BigDataTH_Project\Dota2_BigData\stop-hadoop.ps1
```

Các file chỉ bật/tắt cụm HDFS Dota; không khởi động YARN, không tải lại
dataset và không tắt toàn bộ WSL. Không chạy start và stop đồng thời.

### Vận hành trực tiếp trong Ubuntu

Mở Ubuntu WSL và chạy các lệnh dưới đây. Không cần cài thêm thư viện Python.
Script gắn với user, Hadoop và các đường dẫn trong bảng cấu hình ở trên.

```bash
cd /mnt/d/BigDataTH_Project/Dota2_BigData

# Chỉ cần lần đầu: tạo cấu hình riêng.
python3 scripts/hdfs_cluster_manager.py setup

# Start tự format khi storage NameNode hoàn toàn trống.
# Nếu đã có VERSION thì giữ metadata, không format lại.
python3 scripts/hdfs_cluster_manager.py start

# Tải bằng 16 HTTP Range; ghép và đối chiếu MD5 công bố.
python3 scripts/hdfs_cluster_manager.py download

# Tạo thư mục HDFS và nạp file, không ghi đè file đã có.
python3 scripts/hdfs_cluster_manager.py ingest

# Kiểm tra node, block, replica và SHA-256 toàn bộ file.
python3 scripts/hdfs_cluster_manager.py verify

# Kiểm tra dữ liệu sau khi dừng/bật lại cả cụm.
python3 scripts/hdfs_cluster_manager.py restart-test

# Dừng DN3, đọc toàn bộ file, bật DN3 và kiểm tra lại.
python3 scripts/hdfs_cluster_manager.py failure-test

# Xem trạng thái / dừng cụm khi không sử dụng.
python3 scripts/hdfs_cluster_manager.py status
python3 scripts/hdfs_cluster_manager.py stop
```

Không chạy `setup`, `restart-test`, `failure-test`, `start` hoặc `stop` đồng thời.
Không chạy lệnh format thủ công khi đã có dữ liệu. File cấu hình hiện tại chỉ
có hiệu lực với cụm này khi truyền `--config` hoặc đặt `HADOOP_CONF_DIR`.
Script dùng `nohup` và phiên tiến trình riêng khi start để daemon không nhận
SIGHUP khi phiên lệnh WSL kết thúc. Tắt WSL hoặc khởi động lại Windows vẫn
dừng các daemon; dùng `start` để bật lại.

## Các lệnh HDFS thủ công để demo

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_CONF_DIR=/home/danh/dota-hdfs/conf/namenode
export PATH="$HADOOP_HOME/bin:$PATH"

jps
hdfs dfsadmin -report
hdfs getconf -confKey dfs.blocksize
hdfs getconf -confKey dfs.replication
hdfs dfs -ls -h /dota/raw/zenodo_v3
hdfs dfs -du -h /dota/raw/zenodo_v3
hdfs fsck /dota/raw/zenodo_v3 -files -blocks -locations
```

`getconf` cho biết cấu hình mặc định của client. Kiểm tra `fsck` để biết vị trí
và replica thực tế của file. Đổi replication mặc định không tự đổi file cũ.
Nếu có chủ đích đổi replication của file cũ, dùng `hdfs dfs -setrep -w 2 PATH`.

## Bằng chứng và tiêu chí đạt

Script lưu kết quả kiểm tra vào `D:\BigDataTH_Project\Dota2_BigData\evidence\hdfs` và bản sao
trong `/home/danh/dota-hdfs/evidence`:

- `source_manifest.json`: nguồn, thời điểm tải UTC, dung lượng, MD5, SHA-256, header, số dòng.
- `verification_report.txt`: ba DataNode live.
- `verification_fsck.txt`: HEALTHY, bốn block, hai replica mỗi block.
- `verification_checksum.json`: SHA-256 HDFS trùng nguồn.
- `after_restart_*`: kết quả sau khởi động lại.
- `failure_test.json`: đọc được toàn bộ file khi DN3 dừng.
- `after_failure_*`: trạng thái và checksum sau khi bật lại DN3.

Mở http://localhost:9870, vào Datanodes để quan sát ba node; vào Utilities →
Browse the file system để xem `/dota/raw/zenodo_v3`. Có thể chụp màn hình
những trang này để đưa vào báo cáo. Các file evidence văn bản được script tạo
tự động, không phải ảnh chụp giao diện.

NameNode cần thời gian mới đánh dấu một DataNode đã chết; không yêu cầu giao
diện đổi trạng thái tức thì. Bài kiểm tra lỗi xác minh checksum đọc thực tế.

Nếu startup lỗi, xem log trong `/home/danh/dota-hdfs/logs/<node>/`; kiểm tra
trùng cổng, PID và quyền thư mục. Không ép thoát safe mode hoặc format lại
để bỏ qua lỗi. Script chỉ quản lý cụm Dota, không dừng daemon của cụm khác.

Đầu vào cho bước Spark tiếp theo:

```text
hdfs://localhost:9000/dota/raw/zenodo_v3/DB_03_Live_Features_ANALYSIS_v3.csv
```
