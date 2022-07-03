from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
from airflow.contrib.hooks.aws_hook import AwsHook

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id = "",
                 table_name = "",
                 sql_statement = "",
                 append_data = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.sql_statement = sql_statement
        self.append_data = append_data


    def execute(self, context):
        self.log.info('LoadFactOperator is in progress')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if not self.append_data:
            self.log.info(f"Load data into fact table in Redshift {self.table_name}")
            redshift.run(f'INSERT INTO {self.table_name} {self.sql_statement}') 
            
                
