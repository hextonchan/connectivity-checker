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
"""

import os
import datetime
import subprocess
import sys
from .logging import Logging

LOG = Logging.with_tfns(__name__)

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
        pass

    @classmethod
    def from_json_config(cls, client_path, class_path, trust_store_path, sender, message_type, json_config):
        return cls(
            client_path,
            class_path,
            trust_store_path,
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
            sender,
            message_type
        )

    def set_message(self, message):
        self.__message = message
    
    def set_correlation_id(self, correlation_id):
        self.__correlation_id = correlation_id

    def set_address_line_1(self, address_line_1):
        self.__address_line_1 = address_line_1

    def set_address_line_2(self, address_line_2):
        self.__address_line_2 = address_line_2

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

    def callback(self, message_generation_time = datetime.datetime.now()):

        ## Variable Declaration
        self.__message_generation_time = message_generation_time
        payload = (
            "\r\n\x01"
            + self.__address_line_1
            + "\r\n"
            + self.__address_line_2
            + " {}".format(self.__message_generation_time.strftime("%d%m%y"))
            + "\r\n\x02"
            + self.__message
            + "=\x03") #MQ Accepted Payload Add ddmmyy by 23/02/14
        environ = os.environ.copy()
        environ["CLASSPATH"] = self.__class_path
        message = (
            '{\"sender\": \"' + self.__sender + 
            '\", \"msgGenTime\": \"'+ self.__message_generation_time.strftime("%Y-%m-%dT%H:%M:%S") +
            '\", \"msgType\": \"'+ self.__message_type  +
            '\", \"payload\": \"'+ payload +
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
        command = [
            'java', '-Djavax.net.ssl.trustStoreType=jks',
            f'-Djavax.net.ssl.keyStore={self.__trust_store_path}',
            f'-Djavax.net.ssl.keyStorePassword={self.__trust_store_password}',
            f'-Djavax.net.ssl.trustStore={self.__trust_store_path}',  
            f'-Djavax.net.ssl.trustStorePassword={self.__trust_store_password}', 
            '-Dcom.ibm.mq.cfg.useIBMCipherMappings=false', 
            'Main', f'{message}', '2>&1'
        ]

        LOG.debug(str(type(message)) + ' ' + str(message))

        self.__payload = message

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

            LOG.info("==== [END] MQ Java Subprocess ====")

            return self.__message_generation_time

        except FileNotFoundError:
            err_msg = '[Error 1] Failed to call MQ Java. Missing Library from ' + self.__client_path + '\nDEBUG - ' + str(sys.exc_info()[0]) + ' ' + str(sys.exc_info()[1])
            LOG.error(err_msg)
            raise ImportError(__name__, err_msg)
        except:
            err_msg = '[Error 1] Unknown Exception when calling MQ Java.\nDEBUG - ' + str(sys.exc_info()[0]) + ' ' + str(sys.exc_info()[1])
            LOG.critical(err_msg)
            raise AssertionError(__name__, err_msg)

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
            '\", \"payload\": \"'+ "\r\n\x01" + self.__address_line_1 + "\r\n" + self.__address_line_2  + "\r\n\x02" + self.__message + "=\x03" +
            '\", \"correlationId\": \"'+ self.__correlation_id +
            '\", \"host\": \"' + self.__host +
            '\", \"port\": \"' + self.__port +
            '\", \"queueManagerName\": \"' + self.__queue_manager_name +
            '\", \"channel\": \"' + self.__channel +
            '\", \"queueBsmSendName\": \"' + self.__queue_producer_name +
            '\"}'
        ).encode("utf-8")