'''
    Name:
        msgraph.py
    Author:
        hexton.chan@hkexpress.com
    Description:
        Common Class for Microsoft-Related Topics
    Note:
        20230202 - Init commit
        20230206 - Continuous Implementation
        20230530 - Update log messages
'''

from .msgraph_logging import Logging

import requests, sys
import msal
import os
import base64
import json

DEFAULT_URL = 'https://graph.microsoft.com/v1.0'

### Common Function Call of Requests by Bearer
def get_response_by_bearer(url, token):
    Logging(__name__).info("[requests] GET response with bearer token from " + url)

    try:
        r = requests.get(
            url,
            headers={'Authorization': 'Bearer ' + token['access_token']})

        Logging(__name__).info(str(r.status_code))
        #Logging(__name__).debug(json.dumps(r.json()))
        return r
    except:
        err_msg = '[Error 1] Failed to GET response.\nDEBUG - ' + str(sys.exc_info()[0]) + ' ' + str(sys.exc_info()[1]) + '\n'
        Logging(__name__).error(err_msg)
        for e in r.errors:
            Logging(__name__).error('ERROR: {}'.format(e['err_msg']))
        raise AssertionError(__name__, err_msg)

def post_by_bearer(url, token, request_body):
    Logging(__name__).info("[requests] POST request with bearer token - " + url)

    try:
        r = requests.post(
            url,
            headers={'Authorization': 'Bearer ' + token['access_token']},
            json=request_body)

        Logging(__name__).info(str(r.status_code))
        #print(r.json())
        return r
    except:
        err_msg = '[Error 1] Failed to POST request.\nDEBUG - ' + str(sys.exc_info()[0]) + ' ' + str(sys.exc_info()[1]) + '\n'
        Logging(__name__).error(err_msg)
        for e in r.errors:
            Logging(__name__).error('ERROR: {}'.format(e['err_msg']))
        raise AssertionError(__name__, err_msg)

### Get Bearer Token by msal
def get_token_by_msal(tenant_id, client_id, client_secret, authority, scopes):
    authority = authority + f'/{tenant_id}'

    app = msal.ConfidentialClientApplication(
        client_id=client_id,
        client_credential=client_secret,
        authority=authority)

    Logging(__name__).info("[MSAL] Initalize MS Graph, retrive OAuth Access Token...")

    try:
        token = app.acquire_token_for_client(scopes=scopes)

        if "access_token" in token:
            Logging(__name__).info("[Success 0] Token received. MS Graph is ready to use.")
            return token
        else:
            err_msg = "[Error 1] " + token.get("error") + "\n" + token.get("correlation_id") + "\n" + token.get("error_description")
            Logging(__name__).error(err_msg)
            raise ConnectionRefusedError(__name__, err_msg)
    except:
        err_msg = '[Error 1] get_token_by_msal failed!\nDEBUG - ' + str(sys.exc_info()[0]) + ' ' + str(sys.exc_info()[1]) + '\n'
        Logging(__name__).error(err_msg)
        raise AssertionError(__name__, err_msg)

### Get Message(Email) from Graph, return list (message data only)
def get_message(token, user, url = None, messages = [], top = 10, sender = None, subject = None, from_date = None, to_date = None):
    
    user_id = "('{}')".format(user)

    ## Filter Declaration
    try:
        ## Recurrsion: If function call with url, it came from last execution. Indicated the next message segment.
        if url is not None:
            Logging(__name__).info("[GET] Retrive message via Graph by url: " + url)
        else:
            if sender is not None:
                sender = 'from/emailAddress/address eq \'' + sender + '\''
            if subject is not None:
                subject = 'contains(subject, \''+ subject + '\')'
            if ( (from_date is not None) and isinstance(from_date, str) ) :   #1970-01-01T23:59:59Z
                from_date = 'receivedDateTime ge ' + from_date + 'T00:00:00Z'
            else:
                from_date = 'receivedDateTime ge ' + str(from_date.strftime('%Y-%m-%dT%H:%M:%SZ'))
            
            if ( (to_date is not None) and isinstance(to_date, str) ):    #1970-01-01T23:59:59Z
                to_date = 'receivedDateTime le ' + to_date + 'T23:59:59Z'
            else:
                to_date = 'receivedDateTime le ' + str(to_date.strftime('%Y-%m-%dT%H:%M:%SZ'))    

            filter = ' and '.join([i for i in  [sender, subject, from_date, to_date] if i is not None])
            url = DEFAULT_URL + '/users' + f'{user_id}/messages?count=true&$top={top}&$filter={filter}'

            Logging(__name__).info("[GET] Retrive message via Graph by filter:\n" + url)

    except:
        err_msg = '[Error 1] Failed to cast message URL\nDEBUG - ' + str(sys.exc_info()[0]) + ' ' + str(sys.exc_info()[1]) + '\n'
        Logging(__name__).error(err_msg)
        raise AttributeError(__name__, err_msg)

    r = get_response_by_bearer(url, token)

    if r.ok:
        r = r.json()

        messages.extend(r["value"])

        if( len(messages) < r['@odata.count'] ):
            Logging(__name__).info('Paged response, next URL: ' + str(r['@odata.nextLink']))
            Logging(__name__).info('Total number of cached message: ' + str(len(messages)) + ' out of ' + str(r['@odata.count']))
            get_message(token, user, str(r['@odata.nextLink']), messages)
        else:
            Logging(__name__).info('End of Segment. Total retrieved messages = ' + str(len(messages)))
        
        return messages 
    else:
        err_msg = '[Error 1] Failed to get_message\nDEBUG - ' + str(sys.exc_info()[0]) + ' ' + str(sys.exc_info()[1]) + '\n' + str(r)
        Logging(__name__).error(err_msg)
        raise AssertionError(__name__, err_msg)

def post_message(token, sender, recipient, subject, content, attachment_paths = []):
    
    Logging(__name__).info("[POST] Send message via Microsoft Graph - " + str(subject))
    request_body = {
        'Message': {
            'toRecipients': [{'emailAddress': {'address': i}} for i in recipient],
            'subject': subject,
            'body': {
                'contentType': 'text',
                'content': content
            },
            'attachments' : []
        }
    }
    #for i in recipient: request_body["Message"]["toRecipients"].append({'emailAddress': {'address': i}})

    Logging(__name__).debug('Request Body:\n' + str(request_body))

    if len(attachment_paths) > 0:
        Logging(__name__).info('Total number of attachments: ' + str(len(attachment_paths)))

        for i, file_path in enumerate(attachment_paths):
            Logging(__name__).info(f'[io] Load attachement {str(i)} of {str(len(attachment_paths))} - {str(request_body)}')
            if not os.path.exists(file_path):
                err_msg = '[Error 1] File not found - ' + str(file_path)
                Logging(__name__).error(err_msg)
                raise FileNotFoundError(__name__, err_msg)
            else:
                try:
                    with open(file_path, 'rb') as f:
                        media_content = base64.b64encode(f.read())

                    request_body["Message"]["attachments"].append(
                        {
                            '@odata.type': '#microsoft.graph.fileAttachment',
                            'contentBytes': media_content.decode('utf-8'),
                            'name': os.path.basename(file_path)
                        }
                    )
                except:
                    err_msg = '[Error 1] Failed to load attachment - '
                    Logging(__name__).error(err_msg)
                    raise AssertionError(__name__, err_msg)

    userId = "('{}')".format(sender)
    url = DEFAULT_URL + f'/users{userId}/sendMail'

    r = post_by_bearer(url, token, request_body)
    if r.ok:
        Logging(__name__).info('[Success 0] Message sent.')
        Logging(__name__).debug(request_body["Message"]["body"]["content"])
    else:
        err_msg = '[Error 1] Failed to post_message\nDEBUG - ' + str(sys.exc_info()[0]) + ' ' + str(sys.exc_info()[1]) + '\n' + str(r)
        Logging(__name__).error(err_msg)
        raise AssertionError(__name__, err_msg)
    
    return r

class Graph():
    __user = ""
    __token = ""

    def __init__(self, user = "", token = "") -> None:
        self.__user = user
        self.__token = token
    
    @classmethod
    def connect(cls, service_account_json = {}):
        return cls(
            service_account_json['user'],
            get_token_by_msal(
                service_account_json['tenant_id'],
                service_account_json['client_id'],
                service_account_json['client_secret'],
                service_account_json['authority'],
                service_account_json['scopes'])
        )

    def authorize(self, tenant_id, client_id, client_secret, authority, scopes):
        self.__token = get_token_by_msal(tenant_id, client_id, client_secret, authority, scopes)

    def GET(self, url = None, top = 10, sender = None, subject = None, from_date = None, to_date = None):
        return get_message(self.__token, self.__user, url, [], top, sender, subject, from_date, to_date)

    def POST(self, sender, recipient, subject, content, attachment_paths = []):
        return post_message(self.__token, sender, recipient, subject, content, attachment_paths)