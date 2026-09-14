
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
  # As credenciais do Snowflake serão configuradas via variáveis de ambiente
  # (SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, etc.)
}

resource "aws_s3_bucket" "cnpj_datalake" {
  bucket = "cnpj-pipeline-caiohenrique-datalake"

  tags = {
    Project     = "cnpj-pipeline"
    Environment = "dev"
    ManagedBy   = "terraform"
  }
}