# Kế hoạch phân tích Dota 2 bằng Spark SQL

**Trạng thái: đã triển khai bước 3.** Xem [hướng dẫn chạy và kiểm chứng](STEP3_DRAFT_ANALYTICS.md)
và [báo cáo thực tế](../evidence/step3/step3-20261026-v2/report.md).
Tham khảo phương pháp trong `PREMIER_LEAGUE_ANALYSIS_PLAN.md` do người dùng
cung cấp: chốt câu hỏi, đơn vị phân tích, công thức, coverage, output contract
và kiểm chứng trước khi hiển thị trên website.

## 1. Phần phù hợp và phần cần điều chỉnh

| Ý tưởng từ tài liệu tham khảo | Áp dụng cho Dota 2 |
|---|---|
| Bronze → Silver → Gold → ứng dụng | Giữ `/dota/raw`, `/dota/processed`, `/dota/analytics` |
| Fact theo trận và team-match | Một trận → hai đội → mười hero appearances |
| So sánh có baseline và mẫu số | So sánh hero, bên Radiant/Dire, cặp cùng đội |
| Không tự điền dữ liệu không có | Không tạo patch, mùa, vai trò, player/team ID giả |
| Gold có version, coverage và nguồn | Manifest mỗi lượt phân tích và bảng quality checks |
| Website chỉ hiển thị kết quả đã tính | Đọc bảng Gold; không tự tính lại SQL ở frontend |
| Performance/Price theo phí chuyển nhượng | Không áp dụng: nguồn Dota không có giá hoặc phí |
| So sánh CLB/cầu thủ theo mùa | Không làm với schema hiện tại vì thiếu các định danh và thời gian |
| Neo4j phục vụ quan hệ | Chưa cần cho đồ án cá nhân; hiện giữ Parquet và snapshot JSON phục vụ website |
| Tài liệu tham khảo không có ML | Chỉ áp dụng cho lớp phân tích; Dota vẫn giữ bước ML riêng đã thống nhất |

Không đổi đề tài thành hệ thống chấm điểm tổng hợp hoặc chỉ phân tích mô tả.
Sản phẩm cuối vẫn có phần chọn 10 hero và dự đoán trước trận bằng ML.

## 2. Câu hỏi và phạm vi bước 3

1. Radiant/Dire thắng bao nhiêu trận và tỷ lệ là bao nhiêu?
2. Hero nào được chọn nhiều? Hero nào có tỷ lệ thắng cao khi đủ mẫu?
3. Cùng một hero, kết quả quan sát ở Radiant và Dire khác nhau thế nào?
4. Cặp hero nào thường cùng đội và có tỷ lệ thắng cao khi đủ mẫu?

Chỉ dùng Silver đã kiểm chứng:

```text
hdfs://localhost:9000/dota/processed/match_drafts_v1/
```

Nguồn hiện có 90.530 trận, chỉ gồm `match_id`, `radiant_heroes`, `dire_heroes`,
`radiant_win`. Không đọc trực tiếp một triệu snapshot để đếm hero vì sẽ đếm
một trận nhiều lần. Không phân tích patch, mùa, đội chuyên nghiệp, người chơi
hoặc độ dài trận bằng bảng này. Khi cần mở rộng phải tạo bảng Silver bổ sung.

## 3. Đơn vị tính và công thức

Gọi `N` là số trận hợp lệ trong input của lượt chạy, không hard-code 90.530
trong công thức. Tạo các temporary view trong Spark:

| View | Khóa và số dòng kỳ vọng |
|---|---|
| `matches` | `match_id`; N dòng |
| `team_matches` | `(match_id, side)`; 2N dòng |
| `hero_appearances` | `(match_id, hero_id)`; 10N dòng |
| `pair_appearances` | `(match_id, side, hero_a, hero_b)`; 20N dòng |

Với `team_matches`, `side` là `radiant` hoặc `dire`; `team_win` lần lượt bằng
`radiant_win` hoặc `1-radiant_win`. Hai đội cùng trận phải có tổng win bằng 1.
Một đội năm hero tạo `C(5,2)=10` cặp không thứ tự; lưu `hero_a < hero_b` để
không đếm hai lần `(A,B)` và `(B,A)`. Không tạo tích Descartes các cặp cùng đội.

### Hero

```text
picks(hero)    = số trận có hero
wins(hero)     = tổng team_win của hero
losses(hero)   = picks - wins
pick_rate      = picks / N
win_rate       = wins / picks
```

Pick rate ở đây là tỷ lệ trận chứa hero, không chia cho tổng 10N appearances.
Tổng pick rate mọi hero bằng 10 (tương đương 1.000% khi hiển thị phần trăm).

### Hero theo bên

```text
side_pick_rate = picks(hero, side) / N
side_win_rate  = wins(hero, side) / picks(hero, side)
side_baseline  = wins(side) / N
delta_vs_side_baseline = side_win_rate - side_baseline
```

Chênh lệch là đơn vị tỷ lệ; nhân 100 để hiển thị điểm phần trăm. Kèm số trận
của cả hai phía khi so sánh Radiant với Dire. Khi thiếu một phía, hiển thị
`not_comparable`, không thay tỷ lệ thiếu bằng 0.

### Cặp cùng đội

```text
pair_matches   = số trận có cả hai hero cùng đội
pair_wins      = tổng team_win của cặp
pair_pick_rate = pair_matches / N
pair_win_rate  = pair_wins / pair_matches
```

Tỷ lệ thắng cặp là thống kê đồng xuất hiện, chưa chứng minh hiệu ứng phối hợp
hay quan hệ nhân quả. Không cộng/trung bình các win rate này để tạo xác suất
thắng của một đội hình mới.

## 4. Bảng Gold và hợp đồng đầu ra

Giữ tất cả hero/cặp quan sát được. Mặc định `min_hero_matches=100`,
`min_pair_matches=100`, điều chỉnh qua CLI; ngưỡng chỉ quyết định có được vào
bảng xếp hạng hay không. Dòng dưới ngưỡng vẫn lưu với `insufficient_sample`.

| Bảng | Khóa / trường chính |
|---|---|
| `match_overview` | N, radiant_wins, dire_wins, hai win rate |
| `hero_stats` | hero_id, picks, wins, losses, pick_rate, win_rate, sample_status |
| `hero_side_stats` | hero_id + side, picks, wins, losses, side_pick_rate, side_win_rate, side_baseline, delta_vs_side_baseline, sample_status |
| `hero_pair_stats` | hero_a + hero_b, pair_matches, pair_wins, pair_losses, pair_pick_rate, pair_win_rate, sample_status |
| `analysis_runs` | analysis_run_id, Silver URI, logic_version, Git commit, thời gian UTC, N, ngưỡng mẫu, số dòng mỗi bảng, coverage, limitations |

Các tỷ lệ lưu Double ở khoảng 0–1; chỉ định dạng phần trăm ở lớp báo cáo.
Các bảng số liệu gắn `analysis_run_id`; phép chia có mẫu số 0 trả null và
trạng thái phù hợp, không tạo infinity/NaN. Khi bằng tỷ lệ, xếp ưu tiên số
trận giảm dần rồi hero ID tăng dần để kết quả có thứ tự xác định.

Lưu Parquet Snappy theo mỗi lượt chạy, không partition theo hero/match ID:

```text
/dota/analytics/draft_stats_v1/<analysis_run_id>/
  match_overview/
  hero_stats/
  hero_side_stats/
  hero_pair_stats/
  analysis_runs/
```

Một hoặc hai file dữ liệu cho mỗi bảng là đủ với quy mô này. Không ghi đè
lượt chạy đã có. Chỉ công bố run mới sau khi kiểm tra và đọc lại Gold đạt.
`evidence/step3/<analysis_run_id>/` giữ JSON/Markdown, quality checks và mẫu
top 20; không commit toàn bộ Parquet.

Tên hero là lớp metadata tùy chọn. Nếu bổ sung `dim_hero`, lưu nguồn, phiên
bản và checksum; join không được nhân dòng hoặc làm mất hero. ID chưa có tên
vẫn hiển thị `Hero <id>`. Không dùng metadata hiện tại để suy diễn patch trận.

## 5. Thứ tự triển khai phù hợp làm một mình

1. Entrypoint `analytics/draft_stats.py` chạy một Spark session
   `local[2]`, heap 1 GB, shuffle partitions 8.
2. Kiểm tra bốn cột Silver, khóa duy nhất, đủ 10 hero khác nhau và nhãn 0/1.
3. Tạo view `team_matches`, explode hero rồi tạo cặp bằng vị trí i < j.
4. Dùng `spark.sql` tổng hợp bốn bảng nghiệp vụ theo công thức trên.
5. Tạo manifest/coverage, kiểm tra các invariant và ghi Gold.
6. Đọc lại từng bảng Gold; đối chiếu theo khóa với kết quả trong Spark,
   không so sánh bằng thứ tự vật lý của Parquet.
7. Xuất báo cáo và ca so sánh năm hero được chọn nhiều nhất, xác định từ dữ
   liệu thật; không chọn trước một hero rồi khẳng định nó mạnh nhất.
8. Thêm `analyze-hero-statistics.ps1`, tests và tài liệu kết quả khi bắt đầu triển khai bước 3.

Ưu tiên báo cáo bảng trước. Khi làm website, các biểu đồ nên có: tỷ lệ thắng
hai bên, top 20 lượt chọn, pick rate so với win rate, so sánh hero theo bên và
bảng cặp hero có lọc ngưỡng mẫu. Hiển thị sample size ở mọi bảng xếp hạng.

## 6. Kiểm tra bắt buộc

Với `N=90.530` nếu dùng nguyên Silver v1:

| Kiểm tra | Giá trị kỳ vọng |
|---|---:|
| Team-match rows | 181.060 |
| Hero appearances | 905.300 |
| Cặp cùng đội appearances | 1.810.600 |
| Tổng hero picks | 10N |
| Tổng hero wins | 5N |
| Tổng pair_matches | 20N |
| Tổng pair_wins | 10N |

Ngoài ra:

- Radiant wins + Dire wins = N; tổng các side win rate = 1.
- Mỗi hero: wins + losses = picks; tổng theo bên khớp bảng hero_stats.
- Hero/pair keys duy nhất; hero_a < hero_b; 0 <= wins <= số trận.
- Mọi tỷ lệ hợp lệ; ngưỡng 99 và 100 được kiểm thử riêng.
- Fixture nhỏ có thể tính tay để kiểm tra cách đảo nhãn Dire và đếm cặp.
- Đổi thứ tự hero trong mảng không làm thay đổi aggregate.
- Đổi thứ tự/partition đầu vào cho cùng kết quả số học trong sai số Double.
- Join tên hero thiếu mapping không mất dòng; Gold đọc lại khớp số dòng/khóa.

Coverage báo riêng số trận Silver đầu vào, số trận đã xử lý và số trận bị loại.
Không gọi dữ liệu là bao phủ toàn bộ Dota 2 hoặc meta hiện tại. Nếu input
không đạt schema/invariant đã chốt thì dừng và báo lỗi, không âm thầm lọc.

## 7. Liên hệ với các bước sau

Phân tích Gold dùng toàn bộ Silver để mô tả lịch sử. Bước ML giữ riêng việc
chia tập, fit đặc trưng và đánh giá; không dùng win rate Gold toàn bộ dữ liệu
làm đặc trưng train/test vì sẽ rò rỉ nhãn. Chưa có timestamp nên chưa được
khẳng định đánh giá theo thời gian hoặc theo patch.

Website ban đầu có thể dùng Streamlit + SQLite để đọc aggregate; HDFS/Parquet
vẫn là nguồn tái tạo. Kafka replay cập nhật thống kê ở bước sau phải tách
phần lịch sử ban đầu với phần replay và chống trùng match_id.

Definition of Done bước 3: có bốn bảng thống kê đúng công thức, manifest run,
quality report, bằng chứng kiểm thử, đọc lại Gold thành công và báo cáo truy
vết được về Silver v1. Kết quả được ghi trong báo cáo thực tế, không dựa vào
tài liệu kế hoạch để khẳng định số liệu.
