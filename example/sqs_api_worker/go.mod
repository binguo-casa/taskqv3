module github.com/binguo-casa/taskqv3/example/api_worker/sqs_api_worker

go 1.15

require (
	github.com/aws/aws-sdk-go v1.34.26
	github.com/go-redis/redis/v8 v8.0.0
	github.com/binguo-casa/taskqv3/v3 v3.0.0
)

replace github.com/binguo-casa/taskqv3/v3 => ../..
