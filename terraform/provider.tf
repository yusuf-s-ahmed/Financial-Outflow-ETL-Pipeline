terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.92"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.39.0" # or latest stable version
    }
    
  }

  required_version = ">= 1.2"
}

provider "aws" {
    # Configuration options
    region = "eu-west-2" 
}







