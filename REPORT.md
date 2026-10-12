# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Tạ Quang Dũng |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602588` |
| GitHub | `taquangdung123` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-02689` |

## Kết quả cuối

- `dataset_version` của bạn: `6c42b24ea50b`  (khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `721 / 153 / 145`
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Ở bronze, A dùng `ts_utc` dạng ISO UTC, `speed_mps`, `heading_deg`, `lat`/`lon` và `gas`/`brake`; B dùng `timestamp_ms` dạng epoch mili giây, `spd_kmh`, `hdg`, `latitude`/`longitude` và `throttle_pct`/`brake_pct`. Đồng hồ B còn lệch 1,5 giây so với video. Tôi thấy 3021 dòng A, 3234 dòng B và đúng 2 video; nạp lại không tăng số dòng. Hai bảng bronze riêng giữ nguyên schema nguồn để có thể truy vết; giữ giá trị dạng chữ giúp bảo toàn cả ô trống, giá trị lỗi và đơn vị gốc trước khi chuẩn hóa ở silver.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging đổi tên cột về cùng schema, đổi tốc độ B từ km/h sang m/s, chuyển `timestamp_ms` thành UTC và trừ độ lệch đồng hồ 1500 ms khai báo trong `sources.json`. Sau chuẩn hóa có 2968 dòng sạch A và 3181 dòng sạch B; mẫu đầu của B là `2025-10-08 14:30:00.009+00:00`, cách mốc video 9 ms. Mỗi drive có 20 dòng `duplicate`, 30 dòng `speed_missing`, 3 dòng `speed_out_of_range` trong `silver_telemetry_rejects`. Giữ chúng cùng lý do cách ly cho phép kiểm toán và sửa nguồn; 2968+53=3021 và 3181+53=3234 chứng tỏ không mất dòng âm thầm.

| drive_id | reject_reason | so_dong |
|---|---|---|
| drive_a | duplicate | 20 |
| drive_a | speed_missing | 30 |
| drive_a | speed_out_of_range | 3 |
| drive_b | duplicate | 20 |
| drive_b | speed_missing | 30 |
| drive_b | speed_out_of_range | 3 |

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`max_gap_ms` cảnh báo khoảng mất tín hiệu của cảm biến nhưng dữ liệu còn lại vẫn dùng được; lần chạy này test đó có 1 WARN. `in_range` là lỗi vì tốc độ ngoài 0..70 m/s không được đi tiếp. Trong thí nghiệm `break-gate`, nới luật làm sạch lên 1000 m/s khiến test `in_range_silver_telemetry_speed_mps__70__0` bắt **6 dòng**: `PASS=5 WARN=1 ERROR=1`. Khôi phục luật gốc cho `PASS=6 WARN=1 ERROR=0`. Cổng test độc lập với luật làm sạch đã chặn dữ liệu vô lý lọt vào silver.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Với `trip-00003`, bảng frames có 1246 dòng trước và sau hai lần chạy lại; dấu vân tay vẫn là `6c7480dfde7e`. Trip này có 60 khung, còn cách ghi `append` hai lần tạo 120 dòng ở bảng thử (gấp đôi). Delta `overwrite` theo điều kiện `trip_id` công bố một phiên bản bảng sau khi ghi xong; nếu thao tác lỗi trước commit, phiên bản cũ vẫn đọc được. Xóa trước rồi ghi lại có khoảng thời gian partition rỗng và có thể để lại trạng thái dở dang khi lỗi giữa chừng.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen gần như không có thông tin ảnh nên bị loại (A: 6, B: 4); khung tối vẫn có cảnh hữu ích nên chỉ gắn cờ và giữ (A: 4, B: 61). Tôi chọn ngưỡng dHash **2 bit** như pipeline: nó bắt 149 khung gần trùng ở A và 53 ở B, trong khi ngưỡng 6 có nguy cơ loại quá nhiều khung khác nhau thật. Khi xe chạy chậm, các khung liền nhau giống nhau hơn; vì vậy tốc độ và tần suất lấy mẫu cần được xét khi hiệu chỉnh ngưỡng. Tôi đồng ý giữ khung tối để tránh loại nhầm cảnh đường hầm, nhưng nên kiểm tra bằng ảnh thực tế.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Trong 1019 khung đáng dùng thuộc 26 trip, phép chia theo từng khung làm **26/26 trip** xuất hiện ở nhiều split và cả 26 trip đều có khung ở train lẫn test. Chia theo trip của gold làm **0 trip** bị chia cắt. Các khung gần nhau trong một chuyến có cùng cảnh và điều kiện lái; nếu cảnh đã xuất hiện ở train, test trên khung của chính cảnh đó sẽ cho điểm đẹp giả tạo, không đo được khả năng tổng quát hóa sang chuyến mới.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

`dataset_version` băm danh sách `frame_id|split` theo thứ tự ổn định, nên mô tả chính xác thành viên và cách chia của dataset; ngày giờ hay số đếm có thể đổi dù nội dung không đổi. Hai lượt `export-dataset` liên tiếp đều cho `6c42b24ea50b`, 1019 khung (train 721, val 153, test 145) và cùng `manifest_sha256` `b9d75a19c42a784a67623ecbb1903e59ba4bc5cef677c894e0937502071511db`. MSSV `2A202602588` được trộn vào thứ tự xếp trip nên split, và do đó hash nội dung, khác MSSV khác.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest của `6c42b24ea50b` sau lượt cuối `scripts/lab.sh step all --reset` ghi đầu vào `bronze.raw_telemetry_a`, `bronze.raw_telemetry_b`, `bronze.trips_raw` phiên bản 0 và `bronze.video_files` phiên bản 1; đầu ra trung gian gồm `silver.frames` phiên bản 25 (26 lần ghi, mỗi trip một partition: phiên bản 0..25), `silver.telemetry`, `silver.frames_flagged`, `silver.frame_telemetry` (đều phiên bản 0). SHA-256 video A là `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`, B là `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Để tái tạo, cần đúng hai video, phiên bản bảng đầu vào, MSSV, luật loại khung và luật chia theo trip ghi trong manifest; hash manifest giúp kiểm tra file xuất ra.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

MSSV có chữ nên trip được tính từ phần số: 202602588 % 26 = 6 → phần tử thứ 6 của `list-trips` là `trip-02689`. Với `LAB_FAIL_ONCE=trip-02689`, run `fail1` của `day09_video_pipeline` có **1 task đỏ**: `silver_frames` map index 6 (log: `RuntimeError: simulated crash while extracting trip-02689; run it again`). 25 task `silver_frames` khác xanh; **7 task** phía sau (`dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`) chuyển `upstream_failed`. Tôi chỉ clear `silver_frames[6]` cùng các task hạ nguồn của nó (8 task instance); 25 trip đã xong không chạy lại. Task 6 chạy lần 2 thành công, run chuyển xanh với 37/37 task `success`. `scripts/lab.sh verify` sau đó đạt **27/27**, `frame_id unique` vẫn 1246 khung (không nhân đôi nhờ overwrite theo `trip_id`), `dataset_version` vẫn `6c42b24ea50b` như lượt `normal1` trước khi gây lỗi. Bằng chứng: `report/screenshots/airflow_fail_graph.png`, `fail_task_log.png`, `airflow_graph.png`, `report/verify_after_retry.txt`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

1. **Trích khung bằng ffmpeg trong một container Airflow** (mỗi trip một task trên cùng máy): 500 xe/ngày là hàng chục nghìn trip, CPU và ổ đĩa tạm sẽ nghẽn đầu tiên. Thay bằng xử lý phân tán (Spark/Ray hoặc worker Kubernetes/Celery tự co giãn), có thể dùng GPU decode, và nạp theo sự kiện khi video tới thay vì quét cả lô.
2. **DuckDB một file + Delta ghi đơn luồng**: nhiều task ghi song song vào cùng bảng sẽ xung đột commit, file nhỏ bùng nổ. Thay bằng engine phân tán (Spark/Trino) trên Delta/Iceberg có catalog, partition theo ngày/xe, compaction và Z-order định kỳ.
3. **Airflow dynamic mapping 1 task/trip và chia split bằng toàn bộ danh sách trip**: scheduler sẽ quá tải với hàng chục nghìn task mỗi ngày, và việc xếp lại toàn bộ split làm dataset đổi liên tục. Gom trip theo batch/partition ngày, dùng sensor/sự kiện, và gán split ổn định theo băm `trip_id` (hoặc theo xe/tuyến) để dataset chỉ thêm, không xáo trộn; version dataset theo snapshot.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Câu hỏi: xe chạy chậm có làm mất nhiều khung hơn không? (card "Câu hỏi riêng: tỉ lệ giữ khung theo tốc độ" trên dashboard, ảnh `report/screenshots/metabase_dashboard.png`)

```sql
select case when avg_speed_kmh < 30 then '1. <30 km/h'
            when avg_speed_kmh < 60 then '2. 30-60 km/h'
            else '3. >=60 km/h' end                        as nhom_toc_do,
       count(*)                                            as so_trip,
       sum(frames)                                         as khung,
       sum(frames_kept)                                    as giu_lai,
       round(100.0 * sum(frames_kept) / sum(frames), 1)    as ty_le_giu_pct
from marts.mart_trip_summary
group by 1 order by 1;
```

| nhom_toc_do | so_trip | khung | giu_lai | ty_le_giu_pct |
|---|---|---|---|---|
| <30 km/h | 6 | 256 | 181 | 70.7 |
| 30-60 km/h | 8 | 340 | 279 | 82.1 |
| >=60 km/h | 12 | 650 | 559 | 86.0 |

Trip chậm (<30 km/h) chỉ giữ 70,7% khung, so với 86% ở trip nhanh: khi xe gần như đứng yên, hai khung lấy mẫu cách 0,5 s gần như giống hệt nên bị dHash loại là gần trùng (drive A chậm hơn và có camera đứng hình, nên mất 149 khung trùng so với 53 ở B). Hệ quả: dataset thiên về cảnh chạy nhanh; nếu cần đủ tình huống chạy chậm (kẹt xe, nút giao), nên lấy mẫu theo quãng đường thay vì theo thời gian, hoặc nới ngưỡng dHash theo tốc độ.

## Bonus · Thí nghiệm siết `sync_tol_ms`

Thay đổi: `sync_tol_ms` trong `dbt/dbt_project.yml` từ **150** xuống **50** ms (độ lệch tối đa giữa thời điểm khung và dòng telemetry được ghép). Chạy lại bước 3 và 4, ghi số liệu, rồi khôi phục 150, chạy lại bước 3–5 và `verify`.

| | `sync_tol_ms = 150` (trước, bài nộp) | `sync_tol_ms = 50` (thử) |
|---|---|---|
| Khung mất đồng bộ A / B | 7 / 7 | 272 / 328 |
| Khung trong gold | 1019 | 538 |
| train / val / test (khung) | 721 / 153 / 145 | 376 / 92 / 70 |
| trip train / val / test | 18 / 4 / 4 | 18 / 4 / 4 |
| `dataset_version` | `6c42b24ea50b` | `f070c5a70224` |

Giải thích: telemetry được ghi khoảng 12–13 Hz (mỗi dòng cách nhau ~80 ms, ví dụ `08:00:00.014` → `08:00:00.091`), độ trễ trung bình khung–telemetry là 66 ms (A) và 73 ms (B). Với ngưỡng 50 ms, ngưỡng nhỏ hơn chính chu kỳ lấy mẫu của cảm biến nên gần một nửa số khung hoàn toàn bình thường bị coi là "mất đồng bộ" và gold mất 47% dữ liệu. Ở 150 ms chỉ những khung rơi vào khoảng mất tín hiệu thật (độ trễ tối đa ~3 s) bị loại, 7 khung mỗi drive. Phân chia trip không đổi (chia theo trip, không theo khung) nhưng `dataset_version` đổi vì thành viên dataset đổi — đúng tinh thần băm nội dung. Bài học: ngưỡng đồng bộ phải lớn hơn chu kỳ lấy mẫu telemetry (≥ ~1 chu kỳ cộng biên an toàn), nếu không cổng chất lượng loại dữ liệu tốt. Sau khi khôi phục 150, `verify` đạt 27/27 và `dataset_version` trở lại `6c42b24ea50b` (`report/verify.txt`).

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

- Bất ngờ nhất: chỉ siết `sync_tol_ms` từ 150 xuống 50 ms đã làm gold mất 47% khung (1019 → 538) — một ngưỡng nhìn có vẻ "chặt hơn là tốt hơn" lại loại dữ liệu tốt vì nhỏ hơn chu kỳ ~80 ms của telemetry.
- Cũng bất ngờ: chia theo khung làm cả 26/26 trip rò rỉ sang test; tôi từng nghĩ chia ngẫu nhiên là đủ.
- Clear đúng một task đỏ chỉ chạy lại 1/26 trip mà `dataset_version` vẫn y nguyên — idempotency nhờ overwrite theo `trip_id` thấy rõ khi có lỗi thật.
- Chưa rõ: ngưỡng dHash nên đổi theo tốc độ xe thế nào cho hợp lý; xe chậm (<30 km/h) chỉ giữ 70,7% khung so với 86% khi chạy nhanh.
- Lỗi pipeline có thể chưa bắt: telemetry "đứng" (GPS/tốc độ lặp lại cùng giá trị nhiều giây) vẫn qua `in_range` và `not_null`; khung bị lóa sáng/cháy trắng không bị cờ nào bắt (chỉ có ngưỡng tối/đen).
- Trên Windows, `check_submission.py --save-verify` không gọi được `scripts/lab.sh`, phải chạy `lab.sh verify` qua Git Bash để lưu `verify.txt`.
