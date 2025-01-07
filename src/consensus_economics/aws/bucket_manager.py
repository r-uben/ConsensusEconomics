import boto3

class BucketManager:

    def __init__(self, bucket_name=None) -> None:
        
        self.__bucket_name = bucket_name
        self.__contents = None
        self.s3_client = boto3.client('s3')  # Add this line
    
    @property 
    def bucket_name(self):
        if self.__bucket_name is None: 
            self.__bucket_name = self.set_bucket_name(self.__bucket_name)
        return self.__bucket_name
    
    @property
    def contents(self):
        if self.__contents is None:
            self.__contents = self.get_contents()
        return self.__contents

    def set_bucket_name(self):
        s3 = boto3.client('s3')
        response = s3.list_buckets()
        buckets = [bucket['Name'] for bucket in response['Buckets']]
        print("Available buckets:", buckets)
        bucket_name = input("Please enter a bucket name from the list above: ")
        return bucket_name
    def upload_file(self, file_content, file_path, metadata=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param object_name: S3 object name. If not specified, file_name is used
        """

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=file_path,
                Body=file_content,
                Metadata=metadata if metadata else {}
            )
        except Exception as e:
            print(f"Error uploading {file_path} to S3: {e}")
            return False
        return True

    def remove_file(self, file_path):
        """Remove a file from the S3 bucket
        
        :param file_path: Path to the file in the bucket to remove
        :return: True if successful, False otherwise
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=file_path
            )
            return True
        except Exception as e:
            print(f"Error removing {file_path} from S3: {e}")
            return False


    def get_contents(self):
        s3 = boto3.client('s3')
        paginator = s3.get_paginator('list_objects_v2')
        contents = []
        
        def list_objects(prefix=''):
            for page in paginator.paginate(Bucket=self.bucket_name, Prefix=prefix):
                if 'Contents' in page:
                    contents.extend(page['Contents'])
        
        # Start with no prefix to get everything
        list_objects()
        
        return contents
    
    def get_metadata(self, key):
        """Get metadata for a specific object in the bucket
        
        :param key: S3 object key
        :return: Dictionary containing object metadata
        """
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=key
            )
            return response['Metadata']
        except Exception as e:
            print(f"Error getting metadata for {key}: {e}")
            return None
        
    def get_content(self, key):
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        content = response['Body']
        return content
        
    def read_document(self, key, format=None):
        """Read a document from S3 bucket
        
        :param key: S3 object key (path to file)
        :param format: Format to return ('text', 'bytes', 'dataframe', or 'html')
        :return: Content of the document in specified format
        """
        try:
            content = self.get_content(key)
            if format is None: format = self._get_format(key)

            content_data = content.read()

            if (format == 'text') or (format == 'txt'):
                return content_data.decode('utf-8')
            elif format == 'bytes' or format == 'pdf':
                return content_data
            elif format == 'dataframe':
                return pd.read_csv(io.BytesIO(content_data))
            elif format == 'html':
                return self._parse_html(content_data)
            else:
                raise ValueError(f"Unsupported format: {format}")

        except Exception as e:
            print(f"Error reading document {key}: {e}")
            return None
        
    def _get_format(self, key):
        return key.split('.')[-1]

    def _parse_html(self, content_data):
        """Helper method to parse HTML content."""
        try:
            html_content = content_data.decode('utf-8')
        except UnicodeDecodeError:
            html_content = content_data.decode('utf-8', errors='replace')
        soup = bs4.BeautifulSoup(html_content, 'html.parser')
        return soup