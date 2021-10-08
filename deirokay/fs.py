import re

BUCKET_KEY_REGEX = re.compile(r's3:\/\/([\w\-]+)\/([\w\-\/.]+)')


def split_s3_path(s3_path: str):
    bucket, key = BUCKET_KEY_REGEX.findall(s3_path)[0]
    return bucket, key
