from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 tables=[],
                 dq_checks=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id,
        self.redshift_conn_id = redshift_conn_id,
        self.tables = tables,
        self.dq_checks = dq_checks
        self.dq_tests_repo = {'completeness' : 'SELECT COUNT(*) FROM'} #Here are the possible tests that can be run. You can add more tests as time goes on

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)  # Create a Redshift hook

        test_results = {}
        #Loop through all data quality checks to ensure you are running each one on each table
        for dq_check in dq_checks:
            #Get test name of the current data quality test being run
            test_name = dq_check['test_name']
            #Raise an exception if the specified data quality test is not found
            try:
                #Get the query to run the particular data quality test
                query = self.dq_tests_repo[test_name]
            except Exception as e:
                print(f"Error: {e}. The data quality test you specified can not be found. Check that you have defined thes test correctly. Continuing to the next one.")
                continue #continue to next quality check if test not found
            
            for table in self.tables:  # Iterate over each table
                records = redshift_hook.get_records(query + f' {table}')  # Get the record count for the table
                
                if (
                    eval('len(records) {} {}'.format(dq_check['comparison'],dq_check['expected_result'])) 
                    or eval('len(records[0]) {} {}'.format(dq_check['comparison'],dq_check['expected_result'])) 
                    or eval('records[0][0] {} {}'.format(dq_check['comparison'],dq_check['expected_result']))
                    )
                    error_msg = f"Data quality test {test_name} failed on {table} table"
                    self.log.error(error_msg)
                    self.test_results[table + ' ' + test_name] = {'status': 'failed', 'message': error_msg}
                    raise ValueError(f"Data quality test {test_name} failed on {table} table")
                else
                    success_msg = f"Data quality test {test_name} passed on {table} table with {records[0][0]} records"
                    self.log.info(success_msg)
                    self.test_results[table + ' ' + test_name] = {'status': 'passed', 'message': success_msg}

        return self.test_results