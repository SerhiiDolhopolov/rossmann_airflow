from minio import Minio
from minio.error import S3Error

from config import S3_HOST, S3_PORT, S3_ACCESS_KEY, S3_SECRET_KEY


endpoint = f"{S3_HOST}:{S3_PORT}"
client = Minio(endpoint=endpoint, 
               access_key=S3_ACCESS_KEY,
               secret_key=S3_SECRET_KEY,
               secure=False)

def get_transcation_files(shop_id: int, year: int, month: int, days: list[int]) -> list:
    bucket_name = "transactions"
    try:
        if not client.bucket_exists(bucket_name):
            print(f"Bucket '{bucket_name}' does not exist.")
            return []
        file_list = []
        for day in days:
            file_name = f"shop_{shop_id}/{year:04d}-{month:02d}/trans_rep_shop{shop_id}_{year:04d}-{month:02d}-{day:02d}.csv"
            try:
                file = client.get_object(bucket_name, object_name=file_name)
                if file:
                    file_list.append(file)
            except S3Error as e:
                if e.code == "NoSuchKey":
                    continue
        return file_list
    except S3Error as e:
        print(f"Error occurred: {e}")
        return []