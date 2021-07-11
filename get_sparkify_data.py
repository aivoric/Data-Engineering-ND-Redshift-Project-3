# Read config
# Download S3 files to sparkify folder
# Print the downloaded files

import configparser
import boto3
import os

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY             = config.get('AWS', 'KEY')
SECRET          = config.get('AWS', 'SECRET')
REGION          = config.get('AWS', 'REGION')
LOG_DATA        = config.get('S3', 'LOG_DATA')
LOG_JSONPATH    = config.get('S3', 'LOG_JSONPATH')
SONG_DATA       = config.get('S3', 'SONG_DATA')


class S3Loader:
    def __init__(self):
        self.s3_client = boto3.client('s3',
                                 region_name=REGION,
                                 aws_access_key_id=KEY,
                                 aws_secret_access_key=SECRET)

        self.s3_resource = boto3.resource('s3',
                                 region_name=REGION,
                                 aws_access_key_id=KEY,
                                 aws_secret_access_key=SECRET)
    
    def list_s3_objects(self, bucket):
        object_list = self.s3_client.list_objects_v2(Bucket=bucket)
        for obj in object_list['Contents']:
            print(obj['Key'])
            
    def count_s3_objects(self, bucket):
        bucket = self.s3_resource.Bucket(bucket)
        count_obj = 0
        for i in bucket.objects.all():
            count_obj = count_obj + 1
        return count_obj

    def download_all_files(self, bucket, local):
        """
        params:
        - prefix: pattern to match in s3
        - local: local path to folder in which to place files
        - bucket: s3 bucket with target contents
        """
        keys = []
        dirs = []
        next_token = ''
        base_kwargs = {
            'Bucket':bucket,
        }
        processed_keys = 0
        while next_token is not None:
            kwargs = base_kwargs.copy()
            if next_token != '':
                kwargs.update({'ContinuationToken': next_token})
            results = self.s3_client.list_objects_v2(**kwargs)
            contents = results.get('Contents')
            for i in contents:
                k = i.get('Key')
                if k[-1] != '/':
                    keys.append(k)
                else:
                    dirs.append(k)
            processed_keys += len(contents)
            print(f"Processed keys: {processed_keys}")
            next_token = results.get('NextContinuationToken')
        total_directories = len(dirs)
        total_keys = len(keys)
        print(f"Total directories found: {total_directories}")
        print(f"Total keys found: {total_keys}")
        print("Starting processing directories...")
        for d in dirs:
            dest_pathname = os.path.join(local, d)
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
        print("Starting processing keys...")
        for k in keys:
            dest_pathname = os.path.join(local, k)
            if not os.path.exists(os.path.dirname(dest_pathname)):
                os.makedirs(os.path.dirname(dest_pathname))
            self.s3_client.download_file(bucket, k, dest_pathname)      


if __name__ == "__main__":
    s3 = S3Loader()
    s3.download_all_files('udacity-dend', 'sparkify')