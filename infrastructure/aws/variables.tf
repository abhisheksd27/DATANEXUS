# AWS Provider Configuration Variables

variable "aws_region" {
  description = "AWS Region for provisioning resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name identifier"
  type        = string
  default     = "datanexus"
}

variable "s3_bucket_name" {
  description = "Globally unique name for the S3 Data Lake bucket"
  type        = string
  default     = "datanexus-datalake-analytics-dev"
}

variable "glue_database_name" {
  description = "AWS Glue Catalog Database Name"
  type        = string
  default     = "datanexus_catalog"
}
