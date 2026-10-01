variable "devcloud_endpoint" {
  type = string
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true

  endpoints {
    s3       = var.devcloud_endpoint
    sqs      = var.devcloud_endpoint
    dynamodb = var.devcloud_endpoint
    lambda   = var.devcloud_endpoint
    iam      = var.devcloud_endpoint
    sts      = var.devcloud_endpoint
  }
}
