from prefect.variables import get
import os
import tempfile
import json

class Prefect:
    def __init__(self):
        self.clickhouse_config = get("clickhouse")
        self.clickhouse_creds = None
        self.telegram_config = get("telegram")
        self.telegram_creds = None
        self.gcs_config = get("tevi_gcs_secrets")
        self.gcs_creds = None

    def get_telegram_creds(self):
        if self.telegram_config is None:
            raise ValueError("Telegram config is not set. Please set the config.")
        else:
            self.telegram_creds = {
                "bot_token": self.telegram_config["bot_token"],
                "chat_id": self.telegram_config["chat_id"]
            }
    
    def get_gcs_creds(self):
        if self.gcs_config is None:
            raise ValueError("GCS config is not set. Please set the config.")
        else:
            self.gcs_creds = self.gcs_config

    def get_clickhouse_creds(self):
        if self.clickhouse_config is None:
            raise ValueError("Clickhouse config is not set. Please set the config.")
        else:
            self.clickhouse_creds = {
                "host": self.clickhouse_config["host"],
                "port": self.clickhouse_config["port"],
                "user": self.clickhouse_config["user"],
                "password": self.clickhouse_config["password"],
                "secure": self.clickhouse_config["secure"]
            }

    def set_bqr_env_vars(self, dataset=None):
        self.get_gcs_creds()
        creds = self.gcs_creds
        os.environ["BIGQUERY_PROJECT"] = creds["project"]
        if dataset is None:
            os.environ["BIGQUERY_DATASET"] = creds['dataset']
        else:
            os.environ["BIGQUERY_DATASET"] = dataset
        file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        file.write(json.dumps(creds["keyfile_json"]).encode())
        file.close()
        os.environ["BIGQUERY_KEYFILE"] = file.name

        
    
    def set_clickhouse_env_vars(self):
        self.get_clickhouse_creds()
        creds = self.clickhouse_creds
        os.environ["CLICKHOUSE_HOST"] = creds["host"]
        os.environ["CLICKHOUSE_PORT"] = str(creds["port"])
        os.environ["CLICKHOUSE_USER"] = creds["user"]
        os.environ["CLICKHOUSE_PASSWORD"] = creds["password"]