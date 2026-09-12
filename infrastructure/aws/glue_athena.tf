# AWS Glue Data Catalog & Athena Serverless Data Warehouse

# 1. AWS Glue Catalog Database (Raw Schema)
resource "aws_glue_catalog_database" "raw_db" {
  name        = "${var.glue_database_name}_raw"
  description = "DataNexus Raw Ingestion Schema from S3 Bronze/Gold"
}

# 2. AWS Glue Catalog Database (Staging Schema for dbt)
resource "aws_glue_catalog_database" "staging_db" {
  name        = "${var.glue_database_name}_staging"
  description = "DataNexus Intermediate Staging Schema managed by dbt"
}

# 3. AWS Glue Catalog Database (Analytics Production Star Schema)
resource "aws_glue_catalog_database" "analytics_db" {
  name        = "${var.glue_database_name}_analytics"
  description = "DataNexus Analytics Production Star Schema (Fact & Dimension Tables)"
}

# AWS Athena Workgroup for Serverless SQL Queries
resource "aws_athena_workgroup" "analytics_workgroup" {
  name        = "${var.project_name}-analytics-workgroup"
  description = "Workgroup for DataNexus analytical and BI dashboard queries"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_query_results.bucket}/output/"
    }
  }
}
