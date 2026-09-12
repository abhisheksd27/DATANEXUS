# Amazon S3 Data Lake (Medallion Architecture Storage)

resource "aws_s3_bucket" "datalake" {
  bucket        = var.s3_bucket_name
  force_destroy = true
}

# Enforce S3 Bucket Ownership Controls
resource "aws_s3_bucket_ownership_controls" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Block all Public Access (Enterprise Security Best Practice)
resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# S3 Bucket Versioning for Disaster Recovery & Time Travel
resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# S3 Lifecycle Rule: Transition raw Bronze data to Glacier/Standard-IA after 90 days
resource "aws_s3_bucket_lifecycle_configuration" "datalake_lifecycle" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    id     = "archive-raw-bronze"
    status = "Enabled"

    filter {
      prefix = "bronze/"
    }

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 180
      storage_class = "GLACIER"
    }
  }
}

# 1. Medallion Bronze Zone Placeholder (Raw Immutable Data)
resource "aws_s3_object" "bronze_folder" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "bronze/"
  content = "DataNexus Bronze Raw Zone"
}

# 2. Medallion Silver Zone Placeholder (Cleaned Parquet / Delta Lake)
resource "aws_s3_object" "silver_folder" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "silver/"
  content = "DataNexus Silver Cleaned Zone"
}

# 3. Medallion Gold Zone Placeholder (Business-Ready Aggregations)
resource "aws_s3_object" "gold_folder" {
  bucket  = aws_s3_bucket.datalake.id
  key     = "gold/"
  content = "DataNexus Gold Aggregated Zone"
}

# S3 Bucket for Athena Query Output
resource "aws_s3_bucket" "athena_query_results" {
  bucket        = "${var.s3_bucket_name}-athena-results"
  force_destroy = true
}
