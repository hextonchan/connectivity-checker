"""
    Name:
        mq.py
    Author:
        hexton.chan@hkexpress.com
    Description:
        Rewrite of mq callback Module
    Note:
        20221010 - Init Implementation
        20221024 - Refactored to Object-Oriented
        20221230 - Apply PEP 8
        20230106 - Add Setter for addresses
        20230109 - Refactor for new logging util
        20230112 - Add Error Catching, DEBUG Logging, getter method; Strengthen code security
        20230119 - Re-Def. Msg Gen Time Type
        20230126 - Edited exception handling mechanism
        20230424 - Code fix
        20230503 - Enchancement
        20230524 - Opt Out tfns
"""

import os
import json
import datetime
import subprocess
import sys
from .logging import Logging

LOG = Logging(__name__)

class MQ():
    __client_path = ""
    __class_path = ""
    __trust_store_path = ""
    
    __host = ""
    __port = ""
    __queue_manager_name = ""
    __channel = ""
    __queue_producer_name = ""
    __user_name = ""
    __password = ""
    __trust_store_password = ""

    __address_line_1 = ""
    __address_line_2 = ""

    __sender = ""
    __message_type = ""
    __message_generation_time = datetime.datetime
    __correlation_id = ""
    __message = ""

    __payload = ""

    def __init__( 
            self, 
            client_path = "", 
            class_path = "", 
            trust_store_path = "", 
            host = "", 
            port = "", 
            queue_manager_name = "", 
            channel = "", 
            queue_producer_name = "", 
            user_name = "",
            password = "",
            trust_store_password = "",
            address_line_1 = "",
            address_line_2 = "",
            sender = "",
            message_type = "",
            message_generation_time = datetime.datetime.now(),
            correlation_id = "",
            message = ""
    ) -> None:
        self.__client_path = client_path
        self.__class_path = class_path
        self.__trust_store_path = trust_store_path
        self.__host = host
        self.__port = port
        self.__queue_manager_name = queue_manager_name
        self.__channel = channel
        self.__queue_producer_name = queue_producer_name
        self.__user_name = user_name
        self.__password = password
        self.__trust_store_password = trust_store_password
        self.__address_line_1 = address_line_1
        self.__address_line_2 = address_line_2
        self.__sender = sender
        self.__message_type = message_type
        self.__message_generation_time = message_generation_time
        self.__correlation_id = correlation_id
        self.__message = message

    @classmethod
    def from_json_config(cls, json_config_path, message_type = ''):
        with open(json_config_path, 'rb') as f:
            json_config = f.read()
            
        f.close()
        json_config = json.loads(json_config)
        return cls(
            json_config['path']['client'],
            json_config['path']['java_archives'],
            json_config['path']['trust_store'],
            json_config['host'],
            json_config['port'],
            json_config['queue_manager_name'],
            json_config['channel'],
            json_config['queue_producer_name'],
            json_config['user_name'],
            json_config['password'],
            json_config['trust_store_pwd'],
            json_config['address_ln1'],
            json_config['address_ln2'],
            json_config['sender'],
            message_type
        )

    @classmethod
    def with_secret(cls,
            json_secret,
            path_client = '',
            path_java_archives = '',
            path_trust_store = '',
            address_ln1 = '',
            address_ln2 = '',
            sender = '',
            message_type = ''):
        return cls(
            path_client,
            path_java_archives,
            path_trust_store,
            json_secret['host'],
            json_secret['port'],
            json_secret['queue_manager_name'],
            json_secret['channel'],
            json_secret['queue_producer_name'],
            json_secret['user_name'],
            json_secret['password'],
            json_secret['trust_store_pwd'],
            address_ln1,
            address_ln2,
            sender,
            message_type
        )

    def set_message_type(self, message_type):
        self.__message_type = message_type
    
    def set_message(self, message):
        self.__message = message
    
    def set_correlation_id(self, correlation_id):
        self.__correlation_id = correlation_id

    def set_address_line_1(self, address_line_1):
        self.__address_line_1 = address_line_1

    def set_address_line_2(self, address_line_2):
        self.__address_line_2 = address_line_2

    def set_payload(self, payload):
        self.__payload = payload

    def get_correlation_id(self):
        return self.__correlation_id

    def get_address_line_1(self):
        return self.__address_line_1

    def get_address_line_2(self):
        return self.__address_line_2
    
    def get_message_generation_time(self):
        return self.__message_generation_time

    def get_sender(self):
        return self.__sender

    def get_message_type(self):
        return self.__message_type

    def get_message(self):
        return self.__message

    def get_host(self):
        return self.__host

    def get_channel(self):
        return self.__channel

    def get_payload(self):
        return self.__payload

    def to_string(self):
        return print(
            "correlation_id: " + self.__correlation_id + "\n" +
            "address_line_1: " + self.__address_line_1 + "\n" +
            "address_line_2: " + self.__address_line_2 + "\n" +
            "Message:\n" + self.__message + "\n" +
            "Payload (If sent):\n" + self.__payload
        )
    
    def to_byte(self):
        return (
            '{\"sender\": \"' + self.__sender + 
            '\", \"msgGenTime\": \"'+ self.__message_generation_time.strftime("%Y-%m-%dT%H:%M:%S") +
            '\", \"msgType\": \"'+ self.__message_type  +
            '\", \"payload\": \"'+ "\r\n\x01{}\r\n{} {}\r\n\x02{}=\x03".format(
                self.__address_line_1,
                self.__address_line_2,
                self.__message_generation_time.strftime("%d%m%y"),
                self.__message) +
            '\", \"correlationId\": \"'+ self.__correlation_id +
            '\", \"host\": \"' + self.__host +
            '\", \"port\": \"' + self.__port +
            '\", \"queueManagerName\": \"' + self.__queue_manager_name +
            '\", \"channel\": \"' + self.__channel +
            '\", \"queueBsmSendName\": \"' + self.__queue_producer_name +
            '\", \"userName\": \"' + self.__user_name +
            '\", \"password\": \"' + self.__password +
            '\"}'
        ).encode("utf-8")

    def callback(self, message_generation_time = datetime.datetime.now()):
        ## Variable Declaration
        self.__message_generation_time = message_generation_time
        self.set_payload(self.to_byte())
        
        environ = os.environ.copy()
        environ["CLASSPATH"] = self.__class_path
        command = [
            'java', '-Djavax.net.ssl.trustStoreType=jks',
            f'-Djavax.net.ssl.keyStore={self.__trust_store_path}',
            f'-Djavax.net.ssl.keyStorePassword={self.__trust_store_password}',
            f'-Djavax.net.ssl.trustStore={self.__trust_store_path}',  
            f'-Djavax.net.ssl.trustStorePassword={self.__trust_store_password}', 
            '-Dcom.ibm.mq.cfg.useIBMCipherMappings=false', 
            'Main', f'{self.__payload}', '2>&1'
        ]

        LOG.debug(str(type(self.__payload)) + ' ' + str(self.__payload))

        ## Call MQ Java Object
        try:
            LOG.info("==== [START] MQ Java Subprocess ====")
            output, error = subprocess.Popen(
                command,
                universal_newlines = True,
                env = environ,
                stdout = subprocess.PIPE,
                stderr = subprocess.PIPE,
                cwd = self.__client_path
            ).communicate()
            
            LOG.info('[Success 0] Message: ' + self.__correlation_id + ' successfully sent to remote side.')
            LOG.debug(output)

            if (error != ''):
                LOG.critical(error)
                raise AssertionError(__name__, error)

            LOG.info("==== [END] MQ Java Subprocess ====")

        except FileNotFoundError:
            err_msg = '[Error 1] Failed to call MQ Java. Missing Library from ' + self.__client_path + '\nDEBUG - ' + str(sys.exc_info()[0]) + ' ' + str(sys.exc_info()[1])
            LOG.error(err_msg)
            raise ImportError(__name__, err_msg)
        except:
            err_msg = '[Error 1] Unknown Exception when calling MQ Java.\nDEBUG - ' + str(sys.exc_info()[0]) + ' ' + str(sys.exc_info()[1])
            LOG.critical(err_msg)
            raise AssertionError(__name__, err_msg)
        
        return self.__message_generation_time

        