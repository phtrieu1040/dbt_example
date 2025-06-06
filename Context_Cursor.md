# Hướng dẫn sử dụng Cursor cho dự án Tevi Analytics

## 1. Chạy lệnh SQL
- Khi cần chạy lệnh SQL trực tiếp trên ClickHouse (ví dụ: kiểm tra bảng, query dữ liệu, kiểm tra schema, v.v.), **luôn sử dụng clickhouse MCP** (ClickHouse Management Control Panel hoặc các tool tích hợp MCP).
- Không chạy SQL trực tiếp qua DBT hoặc Python trừ khi có lý do đặc biệt.

## 2. Chạy DBT
- Khi cần thực thi các model DBT (ETL, tạo bảng tổng hợp, build data mart, v.v.), **luôn sử dụng Prefect DBT** (tức là flow Prefect có sử dụng DbtCoreOperation).
- Không chạy lệnh `dbt run` thủ công trên local hoặc CI/CD, mà nên để Prefect quản lý pipeline.

## 3. Quản lý Credential (ClickHouse)
- **Trên môi trường production (Prefect Server/Cloud):**
  - Credential (host, user, password, v.v.) phải được lưu trữ trong Prefect Variable (dạng JSON object, ví dụ tên `clickhouse`).
  - Flow Prefect sẽ lấy credential từ Prefect Variable để đảm bảo bảo mật và dễ quản lý.
- **Trên môi trường local (dev, test):**
  - Credential có thể lấy từ biến môi trường (`os.environ`) hoặc file `.env` (dùng python-dotenv).
  - Nếu không có Prefect Variable, flow sẽ fallback sang biến môi trường.

## 4. Quy tắc tổng quát
- **Không hardcode credential vào code.**
- **Không commit file `.env` chứa credential thực lên git.**
- **Luôn ưu tiên bảo mật và nhất quán giữa các môi trường.**

---

**Tóm tắt:**
- Chạy SQL: dùng clickhouse MCP.
- Chạy DBT: dùng Prefect DBT.
- Credential: prod dùng Prefect Variable, local dùng biến môi trường. 