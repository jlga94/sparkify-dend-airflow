from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    truncate_sql = """
        TRUNCATE TABLE {table};
        COMMIT;
    """
    
    insert_sql = """
        INSERT INTO {table}
        {script};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql_script="",
                 append_data=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql_script = load_sql_script
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not (self.append_data):
            self.log.info(f"Trucating Fact table {self.table} in Redshift")
            formatted_sql = LoadFactOperator.truncate_sql.format(
                table=self.table
            )
            redshift.run(formatted_sql)
        
        self.log.info(f"Loading Fact table {self.table} in Redshift")
        formatted_sql = LoadFactOperator.insert_sql.format(
            table=self.table,
            script=self.load_sql_script
        )
        redshift.run(formatted_sql)
