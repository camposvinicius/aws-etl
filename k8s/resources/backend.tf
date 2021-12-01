terraform {
  backend "s3" {
    bucket = "tfstate-vini-campos"
    key    = "terraform/tfstate"
    region = "us-east-1"
  }
}
