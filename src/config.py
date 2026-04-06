from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    llm_api_key: str
    llm_base_url: str = "https://api.moonshot.cn/v1"
    llm_model: str = "kimi-k2-turbo-preview"

    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_user: str
    mysql_password: str
    mysql_database: str

    doris_host: str = "localhost"
    doris_port: int = 9030
    doris_user: str
    doris_password: str = ""
    doris_database: str

    # Vector store settings
    vector_store_path: str = "data/vector_store"
    embedding_model: str = "BAAI/bge-small-zh"
    use_vector_search: bool = True

    @property
    def mysql_url(self) -> str:
        return f"mysql+pymysql://{self.mysql_user}:{self.mysql_password}@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"

    @property
    def doris_url(self) -> str:
        return f"mysql+pymysql://{self.doris_user}:{self.doris_password}@{self.doris_host}:{self.doris_port}/{self.doris_database}"


@lru_cache
def get_settings():
    return Settings()
