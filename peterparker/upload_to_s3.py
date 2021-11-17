import os
import boto3
import configparser


config = configparser.ConfigParser()
config.read("/home/ec2-user/config.ini")


def run(src_folder, dest_bucket, dest_folder, count):
    resource = boto3.resource(
        's3',
        aws_access_key_id=config["boto"]["aws_access_key_id"],
        aws_secret_access_key=config["boto"]["aws_secret_key"],
        region_name=config["boto"]["region"]
    )

    counter = 0
    for filename in os.listdir(src_folder):
        src_file = f"{src_folder}/{filename}"
        dest_file = f"{dest_folder}/{filename}"

        if counter > count:
            return

        bucket = resource.Bucket(dest_bucket)
        objects = list(bucket.objects.filter(Prefix=dest_file))
        if objects:
            continue
        else:
            resource.Bucket(dest_bucket).upload_file(src_file, dest_file)
            counter += 1
            print(src_file)


if __name__ == "__main__":
    run(
        src_folder="/Users/dindapw/Downloads/series",
        dest_bucket="de-qoala",
        dest_folder="series",
        count=2000
    )
    run(
        src_folder="/Users/dindapw/Downloads/movies",
        dest_bucket="de-qoala",
        dest_folder="movies",
        count=2000
    )
