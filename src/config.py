from functools import lru_cache
from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict


# 获取项目根目录（src/config.py 的上级目录）
PROJECT_ROOT = Path(__file__).parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(PROJECT_ROOT / ".env"),
        env_file_encoding="utf-8",
        extra="ignore"
    )

    llm_api_key: str
    llm_base_url: str = "https://api.moonshot.cn/v1"
    llm_model: str = "kimi-k2-turbo-preview"

    doris_host: str = "localhost"
    doris_port: int = 9030
    doris_user: str
    doris_password: str = ""
    doris_database: str

    # Qdrant settings
    qdrant_host: str = "localhost"
    qdrant_port: int = 6333
    qdrant_collection: str = "schema_embeddings"
    qdrant_fewshot_collection: str = "few_shot_examples"
    qdrant_success_case_collection: str = "success_cases"
    qdrant_vector_size: int = 512

    # Vector store settings
    vector_store_path: str = "workspace/data/vector_store"
    embedding_model: str = "BAAI/bge-small-zh"
    use_vector_search: bool = True

    # Knowledge settings
    knowledge_dir: str = "workspace/data/knowledge"

    @property
    def database_url(self) -> str:
        return f"mysql+pymysql://{self.doris_user}:{self.doris_password}@{self.doris_host}:{self.doris_port}/{self.doris_database}"


@lru_cache
def get_settings():
    return Settings()
