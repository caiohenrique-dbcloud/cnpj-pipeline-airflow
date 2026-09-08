"""
Abstração de armazenamento.

Suporta 3 modos, controlados por variáveis de ambiente:
  1. Disco local (USE_MINIO=false, USE_S3 ausente/false) — para testes rápidos.
  2. MinIO local (USE_MINIO=true) — simula S3 no ambiente de desenvolvimento.
  3. AWS S3 real (USE_S3=true) — produção/nuvem de verdade, via boto3.

O resto do pipeline não precisa saber qual desses três está em uso — só
chama os métodos desta classe (exists, upload_file, download_to, etc.).

Nota de design para o modo S3 real: ao contrário do MinIO (que usa 3
buckets separados: bronze/silver/gold), aqui usamos UM único bucket S3
(mais simples e barato de gerenciar) com prefixos de pasta equivalentes
("bronze/...", "silver/...", "gold/..."). Isso é um padrão comum em
Data Lakes reais na AWS.
"""
import logging
from pathlib import Path

from . import config

logger = logging.getLogger(__name__)


class Storage:
    """Interface única para gravar/ler arquivos, seja em disco, MinIO ou S3 real."""

    def __init__(self):
        self.use_minio = config.USE_MINIO
        self.use_s3 = config.USE_S3

        if self.use_s3:
            import boto3

            from botocore.config import Config as BotoConfig

            boto_retry_config = BotoConfig(
                retries={"max_attempts": 10, "mode": "adaptive"},
                connect_timeout=15,
                read_timeout=60,
            )
            self.s3_client = boto3.client(
                "s3",
                aws_access_key_id=config.AWS_ACCESS_KEY_ID,
                aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
                region_name=config.AWS_REGION,
                config=boto_retry_config,
            )
            self.s3_bucket = config.S3_BUCKET_NAME
            logger.info("Storage configurado para AWS S3 real (bucket: %s)", self.s3_bucket)

        elif self.use_minio:
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
    def _s3_key(self, bucket: str, key: str) -> str:
        """No modo S3 real, 'bucket' (bronze/silver/gold) vira prefixo de pasta
        dentro do único bucket S3 configurado."""
        return f"{bucket}/{key}"

    def exists(self, bucket: str, key: str) -> bool:
        """Checa se um objeto já existe. Base da idempotência do pipeline."""
        if self.use_s3:
            from botocore.exceptions import ClientError

            try:
                self.s3_client.head_object(Bucket=self.s3_bucket, Key=self._s3_key(bucket, key))
                return True
            except ClientError:
                return False

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
        if self.use_s3:
            s3_key = self._s3_key(bucket, key)
            self.s3_client.upload_file(str(local_path), self.s3_bucket, s3_key)
            return f"s3://{self.s3_bucket}/{s3_key}"

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

        if self.use_s3:
            self.s3_client.download_file(self.s3_bucket, self._s3_key(bucket, key), str(local_path))
            return local_path

        if self.use_minio:
            self.client.fget_object(bucket, key, str(local_path))
        else:
            src = self._local_path(bucket, key)
            local_path.write_bytes(src.read_bytes())
        return local_path

    def resolve_path(self, bucket: str, key: str) -> str:
        """Retorna um path que o DuckDB consegue ler diretamente (local ou s3://)."""
        if self.use_s3:
            return f"s3://{self.s3_bucket}/{self._s3_key(bucket, key)}"
        if self.use_minio:
            return f"s3://{bucket}/{key}"
        return str(self._local_path(bucket, key))


    def list_object_names(self, bucket: str, prefix: str) -> list[str]:
        """Lista os nomes dos arquivos (sem o caminho) sob um prefixo, em
        qualquer backend (S3 real, MinIO ou disco local). Centraliza aqui a
        diferença entre backends, para o resto do código não precisar saber
        qual está em uso."""
        if self.use_s3:
            resp = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket, Prefix=self._s3_key(bucket, prefix)
            )
            return [obj["Key"].rsplit("/", 1)[-1] for obj in resp.get("Contents", [])]

        if self.use_minio:
            objects = self.client.list_objects(bucket, prefix=prefix, recursive=True)
            return [Path(obj.object_name).name for obj in objects]

        base = config.LOCAL_DATA_DIR / bucket / prefix.rstrip("/")
        return [p.name for p in base.glob("*.zip")] if base.exists() else []


    def duckdb_configure(self, con):
        """Configura a conexão DuckDB para acessar MinIO ou S3 real, quando aplicável."""
        if self.use_s3:
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute(f"SET s3_region='{config.AWS_REGION}';")
            con.execute(f"SET s3_access_key_id='{config.AWS_ACCESS_KEY_ID}';")
            con.execute(f"SET s3_secret_access_key='{config.AWS_SECRET_ACCESS_KEY}';")
            return
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
