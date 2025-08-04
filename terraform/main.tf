resource "aws_s3_bucket" "pre-operating-expenses-data" {
  bucket = "pre-operating-expenses-data"
}


resource "aws_s3_object" "csv_file" {
    bucket  = aws_s3_bucket.pre-operating-expenses-data.id
    key     = "sample_data.csv"
    source  = "C:\\Users\\yusuf\\Desktop\\terraform-setup\\sample_data.csv"
    etag = filemd5("C:\\Users\\yusuf\\Desktop\\terraform-setup\\sample_data.csv")
}




