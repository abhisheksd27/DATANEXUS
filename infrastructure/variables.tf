variable "project_id" {
  description = "The Google Cloud Platform (GCP) Project ID"
  type        = string
  default     = "datanexus-506318"
}

variable "region" {
  description = "Primary GCP region for regional resources"
  type        = string
  default     = "us-central1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "gcs_bucket_name" {
  description = "Globally unique name for the GCS Data Lake storage bucket"
  type        = string
  default     = "datanexus-datalake-506318"
}