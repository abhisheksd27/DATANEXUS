# AWS Terraform Configuration & Provider Setup

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
    }
  }
}

# ==============================================================================
# IAM Role for Data Pipelines (Optional / Requires Admin IAM permissions)
# NOTE: Commented out because IAM creation requires root/admin IAM permissions.
# Your current IAM user already has full access to execute S3 and Athena queries.
# ==============================================================================

# resource "aws_iam_role" "pipeline_role" {
#   name = "${var.project_name}-pipeline-execution-role"
#
#   assume_role_policy = jsonencode({
#     Version = "2012-10-17"
#     Statement = [
#       {
#         Action = "sts:AssumeRole"
#         Effect = "Allow"
#         Principal = {
#           Service = [
#             "glue.amazonaws.com",
#             "emr.amazonaws.com",
#             "ec2.amazonaws.com"
#           ]
#         }
#       }
#     ]
#   })
# }
#
# resource "aws_iam_role_policy_attachment" "s3_full_access" {
#   role       = aws_iam_role.pipeline_role.name
#   policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
# }
#
# resource "aws_iam_role_policy_attachment" "glue_full_access" {
#   role       = aws_iam_role.pipeline_role.name
#   policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
# }
#
# resource "aws_iam_role_policy_attachment" "athena_full_access" {
#   role       = aws_iam_role.pipeline_role.name
#   policy_arn = "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
# }
