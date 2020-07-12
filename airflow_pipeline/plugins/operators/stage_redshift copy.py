from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftTransfer(BaseOperator):

    template_fields = ()

    template_ext = ()

    ui_color = '#ededed'


    @apply_defaults
    def __init__(
            self,
            
            table,
            s3_bucket,
            s3_key,
            redshift_conn_id='',
            aws_conn_id='',
            
            copy_options=tuple(),
            
            *args, **kwargs):
        super(S3ToRedshiftTransfer, self).__init__(*args, **kwargs)
        
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options
        

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.s3 = S3Hook(aws_conn_id=self.aws_conn_id)
        credentials = self.s3.get_credentials()
        copy_options = '\n\t\t\t'.join(self.copy_options)

        copy_query = """
            COPY {table}
            FROM 's3://{s3_bucket}/{s3_key}/{table}'
            with credentials
            'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
            {copy_options};
        """.format(
                   table=self.table,
                   s3_bucket=self.s3_bucket,
                   s3_key=self.s3_key,
                   access_key=credentials.access_key,
                   secret_key=credentials.secret_key,
                   copy_options=copy_options)

        self.log.info('Executing COPY command...')
        self.hook.run(copy_query)
        self.log.info("COPY command complete...")

