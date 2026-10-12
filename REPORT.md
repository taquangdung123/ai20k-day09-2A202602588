# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | `<điền>` |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `<điền>` |
| GitHub | `<điền: tên tài khoản>` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `<điền: trip-xxxxx>` |

## Kết quả cuối

- `dataset_version` của bạn: `<điền>`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: `<điền>`
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

Manifest của `6c42b24ea50b` ghi đầu vào `bronze.raw_telemetry_a`, `bronze.raw_telemetry_b`, `bronze.trips_raw` phiên bản 0 và `bronze.video_files` phiên bản 1; đầu ra trung gian gồm `silver.frames` (phiên bản 27 sau thí nghiệm chạy lại), `silver.telemetry`, `silver.frames_flagged`, `silver.frame_telemetry` (đều phiên bản 0). SHA-256 video A là `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`, B là `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Để tái tạo, cần đúng hai video, phiên bản bảng đầu vào, MSSV, luật loại khung và luật chia theo trip ghi trong manifest; hash manifest giúp kiểm tra file xuất ra.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

(trả lời ở đây)

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

(trả lời ở đây)

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

(trả lời ở đây)

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

(trả lời ở đây)
