import os
from dotenv import load_dotenv
try:
    load_dotenv(dotenv_path='/home/connectivity-checker/.env', override=True)
except:
    try:
        load_dotenv()
    except:
        pass
    
from utils.logging import Logging
from utils.mq import MQ
from utils.bigquery import BigQuery

logger = Logging(__name__).init()
mq = MQ().from_json_config(os.environ.get('CONFIG_MQ'), 'ASM')
bigquery_dml = BigQuery().connect(os.environ.get('SECRET_GSERVICE_DML'))

from sqls.asm_con import sql

r = bigquery_dml.select_rows_by_standard_sql(sql.get_select_msg_payload('dml-prod'))

mq.set_message(r['payload'][0])
mq.set_correlation_id(r['correlationId'][0])
mq.callback()