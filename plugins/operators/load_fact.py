from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    #Set class attributes
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql="",
                 load_method="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.load_method = load_method

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        #Load data into redshift fact table
        if self.load_method == 'append':
            postgres.run(f"INSERT INTO {self.table} " + self.sql)
        else:
            postgres.run(f'TRUNCATE {self.table}')
            postgres.run(f"INSERT INTO {self.table} " + self.sql)
