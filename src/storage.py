"""
Abstração de armazenamento.

Por que isso existe: o resto do pipeline não deveria precisar saber se está
gravando no MinIO (S3) ou em disco local. Essa camada esconde esse detalhe,
o que também facilita testar o pipeline sem precisar subir o Docker inteiro.
"""
import logging
from pathlib import Path

from . import config

logger = logging.getLogger(__name__)


class Storage:
    """Interface única para gravar/ler arquivos, seja em MinIO ou em disco."""

    def __init__(self):
        self.use_minio = config.USE_MINIO
        if self.use_minio:
            from minio import Minio

            self.client = Minio(
                config.MINIO_ENDPOINT,
                access_key=config.MINIO_ACCESS_KEY,
                secret_key=config.MINIO_SECRET_KEY,
                secure=config.MINIO_SECURE,
            )
            for bucket in (config.BUCKET_BRONZE, config.BUCKET_SILVER, config.BUCKET_GOLD):
                if not self.client.bucket_exists(bucket):
                    self.client.make_bucket(bucket)
                    logger.info("Bucket '%s' criado no MinIO.", bucket)

    # ------------------------------------------------------------------ #
    def exists(self, bucket: str, key: str) -> bool:
        """Checa se um objeto já existe. Base da idempotência do pipeline."""
        if self.use_minio:
            from minio.error import S3Error

            try:
                self.client.stat_object(bucket, key)
                return True
            except S3Error:
                return False
        return self._local_path(bucket, key).exists()

    def upload_file(self, bucket: str, key: str, local_path: Path) -> str:
        """Envia um arquivo local para a camada indicada. Retorna o path final."""
        if self.use_minio:
            self.client.fput_object(bucket, key, str(local_path))
            return f"s3://{bucket}/{key}"

        dest = self._local_path(bucket, key)
        dest.parent.mkdir(parents=True, exist_ok=True)
        if local_path != dest:
            dest.write_bytes(local_path.read_bytes())
        return str(dest)

    def download_to(self, bucket: str, key: str, local_path: Path) -> Path:
        """Baixa um objeto da camada de storage para um caminho local temporário."""
        local_path.parent.mkdir(parents=True, exist_ok=True)
        if self.use_minio:
            self.client.fget_object(bucket, key, str(local_path))
        else:
            src = self._local_path(bucket, key)
            local_path.write_bytes(src.read_bytes())
        return local_path

    def resolve_path(self, bucket: str, key: str) -> str:
        """Retorna um path que o DuckDB consegue ler diretamente (local ou s3://)."""
        if self.use_minio:
            return f"s3://{bucket}/{key}"
        return str(self._local_path(bucket, key))

    def duckdb_configure(self, con):
        """Configura a conexão DuckDB para acessar o MinIO, quando aplicável."""
        if not self.use_minio:
            return
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute(f"SET s3_endpoint='{config.MINIO_ENDPOINT}';")
        con.execute(f"SET s3_access_key_id='{config.MINIO_ACCESS_KEY}';")
        con.execute(f"SET s3_secret_access_key='{config.MINIO_SECRET_KEY}';")
        con.execute(f"SET s3_use_ssl={'true' if config.MINIO_SECURE else 'false'};")
        con.execute("SET s3_url_style='path';")

    # ------------------------------------------------------------------ #
    def _local_path(self, bucket: str, key: str) -> Path:
        return config.LOCAL_DATA_DIR / bucket / key
