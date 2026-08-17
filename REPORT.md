# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Nguyễn Mạnh Hưng  
**Mã sinh viên:** 2A202601829  
**Lớp:** AICB-P2T2  
**Ngày:** 2026-08-17

## 0. Kết quả `make verify`

```text
gold_training_set     ✓ ok   12,480 / 12,480
gold_feature_daily    ✓ ok    9,100 /  9,100
gold_doc_chunks       ✓ ok   31,200 / 31,200
quarantine_tickets    ✓ ok      312 /    312

gold_training_set     8dd7c98653  8dd7c98653  8dd7c98653 ✓
gold_feature_daily    3db448685c  3db448685c  3db448685c ✓
gold_doc_chunks       92d8e50131  92d8e50131  92d8e50131 ✓
quarantine_tickets    ebb89036fb  ebb89036fb  ebb89036fb ✓

dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×)
số file parquet                            ✓ 5,000 → 14
kết quả truy vấn không đổi                 ✓
DAG: catchup / max_active_runs             ✓ False / 1

TỔNG KẾT: 4/4 tiêu chí đạt
```

## 1. Kích thước bảng training tăng sau mỗi lần chạy

**Triệu chứng:** Sau lượt chạy đầu tiên, `gold_training_set` có 13.790 hàng;
sau lượt chạy lại có 26.270 hàng, mặc dù Bronze nhận lại đúng các partition cũ.

**Nguyên nhân:** Bảng có grain một hàng cho một `ticket_id`, nhưng model
incremental không khai báo `unique_key`, nên dbt dùng append. CDC có cả bản ghi
tạo và update; một ticket được chọn ở nhiều ngày và retry tiếp tục ghi thêm
snapshot cũ thay vì cập nhật entity hiện tại.

**Cách khắc phục:** Trong `gold_training_set.sql`, dùng
`unique_key = 'ticket_id'` và `incremental_strategy = 'merge'`. Trong DAG
đặt `catchup=False` và `max_active_runs=1` để tránh chạy bù hàng loạt và
tránh nhiều run đồng thời ghi cùng bảng. `merge` là cách sửa gốc; hai tham số
DAG chỉ giảm khả năng kích hoạt lỗi.

**Bằng chứng:** Trước: 13.790 → 26.270 hàng. Sau: 12.480 hàng; checksum của
ba lượt giống nhau: `8dd7c98653`.

## 2. Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

**Triệu chứng:** `gold_feature_daily` ban đầu có 8.645 hàng thay vì 9.100;
các tổ hợp thiếu tập trung ở ngày cũ.

**P99 độ trễ đo được:** **2.7258333333333336 ngày** (xấp xỉ 65,42 giờ).

**Lookback đã chọn:** **3 ngày**, làm tròn P99 lên theo grain ngày.

**Nguyên nhân:** Điều kiện cũ chỉ lấy `event_date > max(event_date)` trong
Gold. Một event xảy ra ở ngày cũ nhưng được ingest muộn không lớn hơn mốc
`max(event_date)`, nên bị bỏ qua vĩnh viễn.

**Cách khắc phục:** Dùng điều kiện
`event_date >= max(event_date) - interval 3 day`, đồng thời merge theo khóa
ghép `(event_date, customer_id)` để tính lại lookback không tạo hàng trùng.

P99 được dùng thay vì `max` vì `max` có thể bị chi phối bởi một outlier rất
hiếm, khiến phải quét lại một window lớn ở mọi lượt chạy. P99 cân bằng độ bao
phủ và chi phí; phần đuôi vượt P99 cần được theo dõi riêng.

**Bằng chứng:** 8.645 → 9.100 hàng; checksum ổn định là
`3db448685c`.

## 3. Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

**Triệu chứng:** Silver có `NULL` 6.488 hàng và các giá trị ngoài miền như
`-1`, `0`, `5`. Backend đã đổi từ số sang nhãn chữ từ 2026-08-10.

**Nguyên nhân:** `try_cast` chỉ xử lý chuyển kiểu. Nó biến
`urgent/high/medium/low` thành `NULL`, nhưng vẫn chấp nhận `0`, `5`,
`-1` vì chúng là integer hợp lệ về mặt kiểu dữ liệu. Ngoài ra, nếu lọc sau
`row_number`, một update lỗi mới nhất có thể làm mất cả ticket khỏi Silver.

**Ba nhóm giá trị và cách xử lý:**

| Nhóm | Xử lý |
|---|---|
| `1`, `2`, `3`, `4` | Giữ nguyên |
| `urgent`, `high`, `medium`, `low` | Map lần lượt thành `1`, `2`, `3`, `4` |
| `P1`, `P2`, `unknown`, rỗng, `NULL`, `0`, `5`, `-1` | Trả về `NULL`, quarantine |

**Cách khắc phục:** Macro `normalize_priority` dùng `CASE`; Silver lọc
`priority_clean is not null` trước khi xếp hạng; Quarantine dùng cùng macro
để nhặt đúng bản ghi lỗi; schema bật `contract.enforced: true` và thêm test
`not_null`/`accepted_values`.

Dữ liệu lỗi trong Bronze là đúng **312** hàng. Nên giữ lỗi ở Bronze để audit,
nhưng chuẩn hóa và tách lỗi ở Silver để vài trăm bản ghi không chặn toàn bộ
pipeline và dữ liệu hợp lệ.

**Bằng chứng:** `quarantine_tickets = 312`, `silver_tickets.priority`
sạch, `dbt test = 11/11 pass`, Silver vẫn có 12.480 ticket.

## 4. Bài thưởng A — Dashboard chậm

**Nguyên nhân:** Dataset có 5.000 Parquet nhỏ, không partition; query còn bọc
`event_time` trong `strftime`, nên engine phải mở toàn bộ file.

**Cách khắc phục:** `tools/compact.py` sắp xếp theo
`event_date, customer_name`, partition theo `event_date`, dùng row group
10.000 hàng. Query dùng `hive_partitioning` và predicate
`event_date = DATE '2026-08-09'`.

**Bằng chứng:** 130.683 → 130.683 hàng; 5.000 → 14 file; rows scanned
5.000.000 → 9.324, giảm **536,3×**; result hash giữ nguyên
`4379e4c5d9f3`.

## 5. Bài thưởng B — Consumer bị kill giữa batch

**Nguyên nhân:** Code cũ commit offset trước khi ghi, nên crash tạo
at-most-once và có thể mất batch. Đổi sang ghi trước, commit sau tạo
at-least-once; khi đó replay là bình thường nhưng `INSERT` thuần sẽ tạo
duplicate.

**Cách khắc phục:** Thêm primary key trên `event_id`, dùng
`ON CONFLICT (event_id) DO UPDATE`, ghi batch trước rồi mới commit offset.
`DO UPDATE` được chọn thay vì `DO NOTHING` vì message replay có thể chứa nội
dung mới và bản ghi đích cần được cập nhật.

**Bằng chứng:** `make crash-test`: 20.000 hàng chuẩn; sau crash và restart
20.000 hàng, 20.000 `event_id` khác nhau, không mất và không trùng.

## 6. Tổng kết

| Nhiệm vụ | Kiểm tra đầu tiên khi tiếp nhận hệ thống |
|---|---|
| Training | Grain, natural key và chiến lược ghi của incremental model |
| Feature | Chênh lệch giữa thời điểm xảy ra và thời điểm ingest |
| Priority | Phân biệt schema evolution với dữ liệu lỗi thật |
