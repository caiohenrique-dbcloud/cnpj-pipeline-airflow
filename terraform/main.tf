terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.94"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

provider "snowflake" {
  # Credenciais via variáveis de ambiente (~/.snowflake_env):
  # SNOWFLAKE_ORGANIZATION_NAME, SNOWFLAKE_ACCOUNT_NAME,
  # SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ROLE
}

resource "aws_s3_bucket" "cnpj_datalake" {
  bucket = "cnpj-pipeline-caiohenrique-datalake"

  tags = {
    Project     = "cnpj-pipeline"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}

resource "snowflake_warehouse" "cnpj_wh" {
  name                = "CNPJ_WH"
  warehouse_size      = "XSMALL"
  auto_suspend        = 60
  auto_resume         = true
  initially_suspended = true
  comment             = "Warehouse do pipeline CNPJ (XSMALL, suspende em 60s)"
}

resource "snowflake_database" "cnpj_db" {
  name    = "CNPJ_DB"
  comment = "Database do pipeline CNPJ"
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.cnpj_db.name
  name     = "RAW"
  comment  = "Dados brutos carregados do S3"
}