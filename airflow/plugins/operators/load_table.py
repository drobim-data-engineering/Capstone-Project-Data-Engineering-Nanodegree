from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadTableOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 redshift_conn_id='redshift',
                 select_sql='',
                 mode='append',
                 *args, **kwargs):
        super(LoadTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.mode = mode

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")

        if self.mode == 'truncate':
            self.log.info(f'Deleting data from {self.table} table...')
            redshift_hook.run(f'Truncate Table {self.table};')
            self.log.info("Deletion complete.")

        sql = f"""
            INSERT INTO {self.table}
            {self.select_sql};
        """

        self.log.info(f'Loading data into {self.table} table...')
        redshift_hook.run(sql)
        self.log.info("Loading complete.")