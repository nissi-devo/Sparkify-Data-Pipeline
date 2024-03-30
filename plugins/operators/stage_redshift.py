from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="",
                 file_type="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        #Calling __init__ function from parent class BaseOperator to extend functionality
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        #Set class attributes
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.file_type = file_type
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers


  
    def execute(self, context):
        #Load AWS access credentials
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        #Connect to redshift serverless DB
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        #Copy data from S3 buucket to redshift table. 
        #Slightly different implementation for JSON and CSV
        if self.file_type == "json":
            query = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' SECRET_ACCESS_KEY" \
                f" '{credentials.secret_key}' JSON '{self.json_path}' COMPUPDATE OFF"
            redshift.run(query)

        if self.file_type == "csv":

            query = f"COPY {self.table} FROM '{s3_path}' ACCESS_KEY_ID '{credentials.access_key}' " \
                f"SECRET_ACCESS_KEY '{credentials.secret_key}' IGNOREHEADER {self.ignore_headers} " \
                f"DELIMITER '{self.delimiter}'"
            redshift.run(query)
        



