# ==============================================================================
# GCS Bucket for Data Lake (Medallion Architecture Storage)
# NOTE: Currently commented out for local development without GCP Billing.
# Uncomment this block when switching to GCP Cloud Storage.
# ==============================================================================

# resource "google_storage_bucket" "datalake" {
#   name                        = var.gcs_bucket_name
#   location                    = var.region
#   force_destroy               = true
#   uniform_bucket_level_access = true
# 
#   lifecycle_rule {
#     condition {
#       age = 90
#     }
#     action {
#       type          = "SetStorageClass"
#       storage_class = "NEARLINE"
#     }
#   }
# 
#   versioning {
#     enabled = true
#   }
# }
# 
# resource "google_storage_bucket_object" "bronze_folder" {
#   name    = "bronze/"
#   content = "DataNexus Bronze Raw Zone"
#   bucket  = google_storage_bucket.datalake.name
# }
# 
# resource "google_storage_bucket_object" "silver_folder" {
#   name    = "silver/"
#   content = "DataNexus Silver Cleaned Zone"
#   bucket  = google_storage_bucket.datalake.name
# }
# 
# resource "google_storage_bucket_object" "gold_folder" {
#   name    = "gold/"
#   content = "DataNexus Gold Aggregated Zone"
#   bucket  = google_storage_bucket.datalake.name
# }
