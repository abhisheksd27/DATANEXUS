# Terraform Output Values

# output "gcs_bucket_name" {
#   description = "The created GCS Data Lake bucket name". 
#   value       = google_storage_bucket.datalake.name
# }

output "bigquery_raw_dataset" {
  description = "BigQuery raw dataset ID"
  value       = google_bigquery_dataset.raw_dataset.dataset_id
}

output "bigquery_staging_dataset" {
  description = "BigQuery staging dataset ID"
  value       = google_bigquery_dataset.staging_dataset.dataset_id
}

output "bigquery_analytics_dataset" {
  description = "BigQuery analytics dataset ID"
  value       = google_bigquery_dataset.analytics_dataset.dataset_id
}

output "pipeline_service_account_email" {.  
  description = "The email of the pipeline service account"
  value       = google_service_account.pipeline_sa.email
}.  