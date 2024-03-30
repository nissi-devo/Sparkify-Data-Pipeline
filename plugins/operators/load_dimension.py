from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 load_method="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        #Set class attributes
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.load_method = load_method
        
    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #Clear dimension table prior to insert
        if self.load_method == 'overwrite':
            postgres.run(f'TRUNCATE {self.table}')
        
        #Load data into redshift table
        postgres.run(f"INSERT INTO {self.table} " + self.sql)
