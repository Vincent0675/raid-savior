import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minio",
    aws_secret_access_key="minio123",
)

# Listar el primer nivel de prefijos en Silver
response = s3.list_objects_v2(Bucket="silver", Delimiter="/")

print("── Directorios raíz en Silver ──")
for prefix in response.get("CommonPrefixes", []):
    print(" ", prefix["Prefix"])
