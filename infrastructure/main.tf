terraform {
    required_providers {
        aws = {
            source = "hashicorp/aws"
            version = "~> 3.0"
        }
    }
}

variable "access_key" {
    type = string
    sensitive = true
}

variable "secret_key" {
    type = string
    sensitive = true
}

provider "aws" {
    region = "us-east-2"
    access_key = var.access_key
    secret_key = var.secret_key
}
resource "aws_s3_bucket" "lakehouse_bucket" {
    bucket = "sports-lakehouse"

    tags = {
        Name = "Sports Lakehouse"
        Environment = "Personal Project"
    }
}

