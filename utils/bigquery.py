'''
    Name:
        bigquery.py
    Author:
        hexton.chan@hkexpress.com
    Descriptions:
        Common implementation for calling Google Cloud BigQuery Api
        Return a API Client
        Allow parsing rows from Dataframe -> BigQuery
        Implement with custom logging
        Predefined Singleton class
    Note:
        20220531 - Init commit
        20221010 - Add detailed comment
        20221025 - Add exception handling on INSERT
        20221212 - Apply PEP 8
        20230116 - Refactor to new logging style
        20230117 - Re-labeling. Create a Class
        20230126 - Re-designed exception handling mechanism
        20230922 - Rename to bigquery.py
        20231003 - Update log messages
        20231012 - Update wordings
        20240205 - Remove get_service_account, Code refactor, not backward compatible
        20240215 - Update Descriptions, allow multiple type (path/json string/json object) for service_account_json
'''

from google.cloud import bigquery
import os.path
import json

from .logging import Logging
LOG = Logging(__name__)

class Exception():
    def client_not_found():
        err_msg = 'Client not found!\nCalled BigQuery Operation without creating the connection client(section). Please constuct by BigQuery.connect(service_account_key) to establish the connection.'
        LOG.error(err_msg)
        raise ReferenceError(__name__, err_msg)
    
    def service_account_key_not_found():
        err_msg = 'Service Account JSON not found!'
        LOG.error(err_msg)

        raise FileNotFoundError(__name__, err_msg)
    
    def invalid_service_account_json():
        err_msg = 'Invaild Service Account JSON.'
        LOG.error(err_msg)

        raise KeyError(__name__, err_msg)
    
    def exception_connection_client():
        err_msg = 'Failed to initalize the connection client!'
        LOG.error(err_msg)

        raise AssertionError(__name__, err_msg)
    
    def exception_select_sql(response):
        err_msg = 'Failed to excute SELECT SQL on BigQuery.'
        LOG.error(err_msg)
        if response is None:
            raise ConnectionError(__name__, err_msg)
        
        try:
            for e in response.errors:
                LOG.error('ERROR: {}'.format(e['err_msg']))
        except:
            pass
        raise AssertionError(__name__, err_msg)
    
    def exception_insert_sql(job_history):
        err_msg = 'Failed to INSERT Dataframe to BigQuery, mostly due to SQL Exception.\nBigQuery Job Summary:\n{}\n'.format(str(job_history))
        LOG.error(err_msg)
        LOG.error(job_history)
        raise AssertionError(__name__, err_msg)
    
    def system_error_insert_sql():
        err_msg = 'Failed to INSERT Dataframe to BigQuery, mostly due to System Error.\n'
        LOG.error(err_msg)
        raise SystemError(__name__, err_msg)

def connect_bigquery(service_account_json):
    ### Connect to BigQuery, create section, return the connection client, accept os.PathLike or service account json
    try:
        if os.path.isfile(service_account_json):
            io = open(service_account_json)
            service_account_json = json.load(io)
            io.close()
        elif isinstance(service_account_json, str):
            service_account_json = json.loads(service_account_json.replace("'", "\""))
        else:
            service_account_json = json.load(service_account_json)
            
        LOG.info('Establish connection: ' + service_account_json['project_id'])
        
        client = bigquery.Client.from_service_account_info(service_account_json)
        
        LOG.info('Session created. ' + client.project.title())
        
        return client
    except FileNotFoundError:
        Exception.service_account_key_not_found()
    except KeyError:
        Exception.invalid_service_account_json()
    except:
        Exception.exception_connection_client()

def select_rows_by_standard_sql(client, sql):
    ### Select rows by execute Standard SQL on BigQuery, return pandas.Dataframe()
    response = None
    
    LOG.info('Execute SQL on {}:\n{}\n'.format(client.project.title(), sql))
    
    try:
        response = client.query(sql)
        if response.ended and response.started:
            LOG.info('SQL executed. Timelapsed: ' + str(response.ended - response.started))
            
        df = response.result().to_dataframe()
            
        LOG.info('Retrieved {} rows.\n'.format(len(df)))
        LOG.debug('\n{}\n'.format(df.to_string()))

        return response.result().to_dataframe()
    except: 
        Exception.exception_select_sql(response)

def insert_rows_by_dataframe(client, table_id, dataframe):
    ### Insert many rows from pandas Dataframe to Google BigQuery, return the job history(Google's format)
    
    job_history = [[]]  #Google's format. [[]] means OK
    
    LOG.info('INSERT {} rows of Dataframe to BigQuery Table - {}'.format(
        str(len(dataframe)),
        table_id))
    
    LOG.debug('\n' + dataframe.to_string())
        
    try:
        job_history = client.insert_rows_from_dataframe(client.get_table(table_id), dataframe)

        if (len(job_history[0]) > 0 ):
            Exception.exception_insert_sql(job_history)
        else:
            LOG.info('INSERT instruction sent to Stream API.')
            LOG.info(job_history) #Google's format. [[]] means OK
    except:
        Exception.system_error_insert_sql()

    return job_history

class BigQuery():
    __client = None

    def __init__(self, client = None) -> None:
        self.__client = client

    @classmethod
    def connect(cls, service_account_json : str):
        return cls(
            connect_bigquery(service_account_json)
        )

    def select_rows_by_standard_sql(self, sql):
        if self.__client is None:
            Exception.client_not_found()

        return select_rows_by_standard_sql(self.__client, sql)

    ### Insert many rows from pandas Dataframe to Google BigQuery
    def insert_rows_by_dataframe(self, table_id, dataframe):
        if self.__client is None:
            Exception.client_not_found()

        return insert_rows_by_dataframe(self.__client, table_id, dataframe)