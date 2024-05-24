"""
    Name:
        logging.py
    Authors:
        hexton.chan@hkexpress.com
    Descriptions:
        Custom logging class
    Notes:
        20221010 - Add Comment
        20221024 - Redefine Config
        20221228 - Add tfns related integration
        20230103 - Modify the log messages
        20230109 - Support Custom Logger, remove legacy function, full code refactoring
        20230112 - Update Descriptions
        20230119 - Update tfns
        20230529 - Add directory auto creation
        20230619 - Enchancement
        20230620 - BugFix @with_tfns()
        20230822 - Add compatibility for env variable
        20230823 - Clean up and bug fix
        20230922 - Enchancement, not backward compatible; Update descriptions
        20230925 - Add DEFAULTs
        20231123 - Show thread_id
        202402 - Enchanced function call
        20240216 - Rewrite Core; Disabled .conf; Remove Zabbix tfns from core, TBI on another pipeline
        20240307 - BugFix
        20240417 - Add back get_logger() for legacy code
        20240517 - Email logger bugfix
"""

import os
import json
import sys
import datetime
import warnings
import threading
import logging
import logging.config

### DEFAULT ENVIRONMENT VARIABLES
if not os.environ.get('LOG_LEVEL'): os.environ['LOG_LEVEL'] = 'INFO'

if not os.environ.get('LOG_FILE_HANDLER') and not os.environ.get('LOG_FILE_DIR') and not os.environ.get('LOG_FILE_NAME'):
    ## If Log file's directory and name are set with default value, disabled file handler
    os.environ['LOG_FILE_HANDLER'] = 'disabled'
else:
    ## If Log file's directory and name are set with non-default value, enable file handler
    os.environ['LOG_FILE_HANDLER'] = 'enabled'
if not os.environ.get('LOG_FILE_DIR'): os.environ['LOG_FILE_DIR'] = os.path.join(os.getcwd(), 'log').replace('\\', '/')
if not os.environ.get('LOG_FILE_NAME'): os.environ['LOG_FILE_NAME'] = str(datetime.datetime.today().date()) + '.log'
if not os.environ.get('LOG_FILE_PATH'): os.environ['LOG_FILE_PATH'] = (os.environ['LOG_FILE_DIR'] + '/' + os.environ['LOG_FILE_NAME']).replace('\\', '/')

### Import MSGraph.Message if available
if os.environ.get('SECRET_MSGRAPH') and os.environ.get('PATH_MESSAGE_CONFIG') and os.environ.get('PATH_MESSAGE_CONFIG'):
    try:
        from utils.msgraph import Graph as msgraph

        MESSAGE = msgraph.connect(json.loads(open(os.environ['SECRET_MSGRAPH']).read()))
        MESSAGE_CONFIG = json.loads(open(os.environ.get('PATH_MESSAGE_CONFIG')).read())['log']
    except:
        print('Failed to enable the feature to send log message via msgraph.\nDEBUG - {} {}'.format(str(sys.exc_info()[0]), str(sys.exc_info()[1])))
        del os.environ['ENABLE_MAIL_LOGGER']
        pass

class _Exception():
    def __init__(self) -> None:
        pass
    
    def file_handler_unavailable():
        warnings.warn(
            'Requested Logger with FileHandler but unavailable. Logger continue stream without write files.\n' 
            + 'This may cause as deployed ENV has a read-only file system or unknown error occurred.\nDEBUG - {} {}\n'.format(
                str(sys.exc_info()[0]), str(sys.exc_info()[1])), ImportWarning, stacklevel=4)
        
def unicode(text):
    if str.isascii(str(text)):
        return str(text)
    else:
        return str(text).encode('utf-8')

def get_logger(classname = __name__) -> logging.Logger:
    logging.getLogger(classname).handlers.clear()

    logger = logging.getLogger(classname)
    logger.setLevel(os.environ.get('LOG_LEVEL'))
    
    formatter = logging.Formatter(
        '[thread_id {}] %(asctime)s - %(name)s - %(levelname)s - %(message)s'.format(threading.get_ident()),
        )
    
    ## Stream Handler
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(os.environ.get('LOG_LEVEL'))
    logger.addHandler(stream_handler)
    
    ## File Handler
    if os.environ.get('LOG_FILE_HANDLER') == 'enabled':
        try:
            if not os.path.exists(os.environ['LOG_FILE_DIR']) : os.makedirs(os.environ['LOG_FILE_DIR'])
            
            file_handler = logging.FileHandler(
                filename=os.environ.get('LOG_FILE_PATH'),
                mode='a',
                encoding='utf-8')
            file_handler.setFormatter(formatter)
            file_handler.setLevel(os.environ.get('LOG_LEVEL'))
            
            logger.addHandler(file_handler)
        except:
            _Exception.file_handler_unavailable()
    
    return logger
    
class Logging():
    __classname = __name__

    def __init__(self, classname = __classname) -> None:
        self.__classname = classname

    def get_classname(self):
        return self.__classname

    def set_classname(self, classname):
        self.__classname = classname
        
    def get_logger(self):
        return get_logger(self.get_classname())
        
    def init(self, message = "Initalize..."):
        get_logger(self.get_classname()).info(unicode(message))
        return self

    def info(self, message):
        get_logger(self.get_classname()).info(unicode(message))

    def error(self, message):
        #print(unicode(message + '\n"{} {}"\n'.format()))
        
        get_logger(self.get_classname()).error(unicode(message + '\n"{} {}"\n'.format(
                str(sys.exc_info()[0]), str(sys.exc_info()[1]))))
        
        try:
            if os.environ.get('ENABLE_MAIL_LOGGER'):
                MESSAGE.POST(
                    MESSAGE_CONFIG['error']['sender'],
                    MESSAGE_CONFIG['error']['recipient'],
                    MESSAGE_CONFIG['error']['subject'],
                    unicode(message + '\nDEBUG - "{} {}"\n'.format(str(sys.exc_info()[0]), str(sys.exc_info()[1]))))
        except:
            print('Failed to send critical log message to msgraph, or feature disabled.\nDEBUG - {} {}'.format(str(sys.exc_info()[0]), str(sys.exc_info()[1])))
            pass

    def warning(self, message):
        get_logger(self.get_classname()).warning(unicode(message))

    def debug(self, message):
        get_logger(self.get_classname()).debug(unicode(message))

    def critical(self, message):
        #print(unicode(message + '\n"{} {}"\n'.format()))
        
        get_logger(self.get_classname()).critical(unicode(message + '\n"{} {}"\n'.format(
                str(sys.exc_info()[0]), str(sys.exc_info()[1]))))
        
        try:
            if os.environ.get('ENABLE_MAIL_LOGGER'):
                MESSAGE.POST(
                    MESSAGE_CONFIG['critical']['sender'],
                    MESSAGE_CONFIG['critical']['recipient'],
                    MESSAGE_CONFIG['critical']['subject'],
                    unicode(message + '\nDEBUG - "{} {}"\n'.format(str(sys.exc_info()[0]), str(sys.exc_info()[1]))))
        except:
            print('Failed to send critical log message to msgraph, or feature disabled.\nDEBUG - {} {}'.format(str(sys.exc_info()[0]), str(sys.exc_info()[1])))
            pass

    def EOF(self, message = "End of file."):
        get_logger(self.get_classname()).info(unicode(message))