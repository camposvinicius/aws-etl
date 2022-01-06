terraform {
  backend "s3" {
    bucket = "tfstate-vini-campos-etl-aws-poc"
    key    = "terraform/tfstate"
    region = "us-east-1"
  }
}
