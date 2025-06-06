# Contributing to Flows

This document outlines the procedures for adding, updating, and deleting flows in this project. Flows are defined using the Prefect library.

## Directory Structure

Flows are organized within the `flows/` directory. Environment-specific configurations or flows can be placed in the `flows/staging/` and `flows/production/` subdirectories.

## Adding a New Flow

1.  **Create a new Python file** for your flow within the `flows/` directory (or the appropriate environment subdirectory).
    *   Follow the naming convention `your_flow_name.py`.
2.  **Define your flow** using the `@flow` decorator from the `prefect` library.
    *   Refer to `flows/hello_world.py` for a basic example.
    *   Ensure your flow has a descriptive name.
3.  **Define tasks** within your flow using the `@task` decorator.
4.  **Add an entry point** for local execution, if necessary:
    ```python
    if __name__ == "__main__":
        your_flow_name() # Or with specific parameters
    ```
5.  **Test your flow locally** to ensure it runs as expected.
6.  **(Optional) Update `prefect.yaml`**: If your new flow needs to be registered with Prefect for deployments, you might need to update the `prefect.yaml` file. This step depends on your project's deployment strategy.
7.  **Commit and push** your changes.

## Updating an Existing Flow

1.  **Locate the Python file** for the flow you want to update in the `flows/` directory.
2.  **Make the necessary modifications** to the flow logic or tasks.
3.  **Test the changes locally.**
4.  **Commit and push** your changes.

## Deleting a Flow

1.  **Identify the Python file** corresponding to the flow you want to delete in the `flows/` directory.
2.  **Delete the file.**
3.  **(Optional) Update `prefect.yaml`**: If the flow was registered in `prefect.yaml`, remove its entry.
4.  **Commit and push** your changes.

## Best Practices

*   Keep flows focused on a single, well-defined purpose.
*   Use descriptive names for flows and tasks.
*   Add comments to explain complex logic.
*   Write unit tests for your tasks where appropriate.
*   Ensure your flow handles potential errors gracefully.

---

To make this even better, you might want to add sections about:

*   **Deployment**: How are these flows deployed to staging/production? (This might involve the `prefect.yaml` file, GitLab CI/CD, Docker, etc.)
*   **Configuration**: How is configuration (e.g., API keys, database credentials) managed for flows?
*   **Dependencies**: How are Python dependencies for flows managed (e.g., `requirements.txt`, `pyproject.toml`)?

What do you think? Should we add or change anything?

---

## Prefect-DBT Sample: tevi_analytics

### 1. Cài đặt dependencies

```bash
pip install -r requirements.txt  # hoặc sử dụng pyproject.toml với poetry/pdm/uv
```

### 2. Cấu trúc thư mục mẫu

- tevi_analytics/
  - dbt_project.yml
  - profiles.yml
  - models/
    - example.sql
- flows/
  - dbt_tevi_flow.py

### 3. Chạy thử flow Prefect với DBT

```bash
python flows/dbt_tevi_flow.py
```

Flow sẽ chạy lệnh `dbt run` trên project tevi_analytics sử dụng DuckDB (file tevi_analytics.duckdb sẽ được tạo tự động).

### 4. Tuỳ chỉnh cấu hình DBT
- Sửa file `tevi_analytics/profiles.yml` nếu muốn đổi đường dẫn database hoặc cấu hình khác.
- Thêm/sửa models trong `tevi_analytics/models/` để mở rộng project.

### 5. Tham khảo thêm
- [Prefect-DBT documentation](https://prefecthq.github.io/prefect-dbt/)
- [DBT DuckDB adapter](https://docs.getdbt.com/reference/warehouse-profiles/duckdb-profile)
