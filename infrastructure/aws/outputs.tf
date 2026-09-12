# AWS Terraform Outputs

output "s3_datalake_bucket_name" {
  description = "The Amazon S3 Data Lake Bucket Name"
  value       = aws_s3_bucket.datalake.bucket
}

output "s3_athena_results_bucket" {
  description = "S3 bucket for Athena query output results"
  value       = aws_s3_bucket.athena_query_results.bucket
}

output "glue_raw_database" {
  description = "AWS Glue Raw database name"
  value       = aws_glue_catalog_database.raw_db.name
}

output "glue_staging_database" {
  description = "AWS Glue Staging database name"
  value       = aws_glue_catalog_database.staging_db.name
}

output "glue_analytics_database" {
  description = "AWS Glue Analytics database name"
  value       = aws_glue_catalog_database.analytics_db.name
}

output "athena_workgroup_name" {
  description = "AWS Athena analytical workgroup name"
  value       = aws_athena_workgroup.analytics_workgroup.name
}

# output "pipeline_iam_role_arn" {
#   description = "IAM Role ARN for DataNexus pipelines"
#   value       = aws_iam_role.pipeline_role.arn
# }
