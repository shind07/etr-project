provider "aws" {
  region = "us-east-1" 
}

resource "random_string" "random" { 
  length  = 8
  special = false
  upper   = false
}

resource "aws_s3_bucket" "etr_data" {
  bucket = "etr-data-${random_string.random.result}"
}

resource "aws_s3_bucket_versioning" "etr_data_versioning" {
  bucket = aws_s3_bucket.etr_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "etr_data_encryption" {
  bucket = aws_s3_bucket.etr_data.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_ecr_repository" "etr_app" {
  name = "etr-app-${random_string.random.result}"
  
  image_scanning_configuration {
    scan_on_push = true
  }
}

resource "aws_ecr_lifecycle_policy" "etr_app_lifecycle" {
  repository = aws_ecr_repository.etr_app.name

  policy = jsonencode({
    rules = [{
      rulePriority = 1
      description  = "Keep last 5 images"
      selection = {
        tagStatus   = "any"
        countType   = "imageCountMoreThan"
        countNumber = 5
      }
      action = {
        type = "expire"
      }
    }]
  })
}

output "s3_bucket_name" {
  description = "Name of the created S3 bucket"
  value       = aws_s3_bucket.etr_data.id
}

output "ecr_repository_url" {
  description = "URL of the ECR repository"
  value       = aws_ecr_repository.etr_app.repository_url
}
