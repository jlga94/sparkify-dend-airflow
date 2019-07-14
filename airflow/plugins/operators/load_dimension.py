from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
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

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql_script = load_sql_script
        self.append_data = append_data

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if not(self.append_data):
            self.log.info(f"Trucating Dimension table {self.table} in Redshift")
            formatted_sql = LoadDimensionOperator.truncate_sql.format(
                table=self.table
            )
            redshift.run(formatted_sql)
        
        self.log.info(f"Loading Dimension table {self.table} in Redshift")
        formatted_sql = LoadDimensionOperator.insert_sql.format(
            table=self.table,
            script=self.load_sql_script
        )
        redshift.run(formatted_sql)
