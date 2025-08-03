resource "aws_s3_bucket" "pre-operating-expenses-data" {
  bucket = "pre-operating-expenses-data"
}


resource "aws_s3_object" "csv_file" {
    bucket  = aws_s3_bucket.pre-operating-expenses-data.id
    key     = "Largence CF to 31-07-2025.csv"
    source  = "C:\\Users\\yusuf\\Desktop\\Largence\\Data\\Largence CF to 31-07-2025.csv"
    etag = filemd5("C:\\Users\\yusuf\\Desktop\\Largence\\Data\\Largence CF to 31-07-2025.csv")
}




