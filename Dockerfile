FROM prefecthq/prefect:3.4.1-python3.11
# /builds/tevi/data/pipelines/ is pwd of `prefect deploy` which is gitlab runner pwd
COPY pyproject.toml /builds/tevi/data/pipelines/
RUN pip install uv && uv pip install --system -r /builds/tevi/data/pipelines/pyproject.toml
COPY . /builds/tevi/data/pipelines/
WORKDIR /builds/tevi/data/pipelines/
