import os, json , base64
from utils.requests import request

os.environ['REQUEST_MAXIMUM_RETRIES'] = '0'
os.environ['DISABLE_LOGGING_EMAIL'] = '1'
os.environ['SECRET_AMOS'] = '/etc/opt/secrets/amos/amos-aim-test.json'

amos = json.loads(open(os.environ['SECRET_AMOS']).read())

try:
  r = request(
    method = 'GET',
    url = amos['url'] + '/actualStateRequest',
    verify = False,
    headers = {'Authorization': 'Basic {}'.format(base64.b64encode('{}:{}'.format(amos['username'], amos['password']).encode('utf-8')).decode('utf-8'))})
except Exception as e:
  r = e.args[2]

print(r.text)
