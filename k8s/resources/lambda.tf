resource "aws_iam_role" "iam_for_lambda" {
  name = "iam_for_lambda"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": [
          "lambda.amazonaws.com"
        ]
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}

resource "aws_iam_policy" "policy" {
  name = "iam_for_lambda_policy"

  policy = <<-EOF
{
    "Version": "2012-10-17",
    "Statement": [
      {
        "Sid": "Stmt1590217939128",
        "Action": "s3:*",
        "Effect": "Allow",
        "Resource": "*"
      }
    ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "policy-attach" {
  role       = aws_iam_role.iam_for_lambda.name
  policy_arn = aws_iam_policy.policy.arn
}

resource "aws_lambda_function" "lambda_function" {
  filename      = "lambda_function.zip"
  function_name = "myfunction"
  role          = aws_iam_role.iam_for_lambda.arn
  handler       = "lambda_function.lambda_handler"
  memory_size   = 256
  timeout       = 60

  source_code_hash = filebase64sha256("lambda_function.zip")

  runtime = "python3.9"

}