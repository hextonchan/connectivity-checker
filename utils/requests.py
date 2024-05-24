"""
    Name:
        requests.py
    Author:
        hexton.chan@hkexpress.com
    Description:
        Common HTTP Request Util.
    Notes:
        20231026 - Enchancement, not backward compatible; Update descriptions
        20231120 - Refactoring
        20240205 - Bug Fix on Exception Handling, introduce kwargs; Not backward compatible
        202404 - Debug, code refactoring
        20240419 - Bug Fix
"""
import os
import sys
import time
import requests

from .logging import Logging
LOG = Logging(__name__)

if os.environ.get('REQUEST_MAXIMUM_RETRIES'):
    MAXIMUM_RETRIES = int(os.environ.get('REQUEST_MAXIMUM_RETRIES'))
else:
    MAXIMUM_RETRIES = 3
    
def get_requests():
    return requests

def request(method, url, **kwargs):
    ### Request with auto retry
    response = None
    retries = 0
    
    while True: 
        try:
            LOG.info('[{}] {}\nkwargs: {}\n'.format(method, url, str(kwargs)))
            
            response = requests.request(method, url, **kwargs)

            if response is not None:
                LOG.info('{} {}'.format(str(response.status_code), str(response.reason)))

                if not response.ok:
                    LOG.error(str(response.text))
                    raise ConnectionError(__name__)
            else:
                raise AssertionError(__name__)

            break
        
        except Exception as e:
            err_msg = ('[{}/{}] Failed to [{}] {}'.format(
                str(retries),
                str(MAXIMUM_RETRIES),
                method,
                url,
                ))
                
            LOG.error('{}\n{}'.format(str(err_msg), str(e)))
            LOG.debug('{} {}'.format(str(sys.exc_info()[0]), str(sys.exc_info()[1])))
            
            if retries < MAXIMUM_RETRIES:
                LOG.info('Retry after 1 second.')
                time.sleep(1)
                retries = retries + 1
                continue
            else:
                LOG.critical('Reached maximum {} retries.'.format(str(MAXIMUM_RETRIES)))
                raise AssertionError(__name__, err_msg, response)
            
    return response