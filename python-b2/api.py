import hashlib
import json

import requests
from requests.auth import HTTPBasicAuth

AUTHORIZE_ACCOUNT = 'https://api.backblaze.com/b2api/v1/b2_authorize_account'
GET_UPLOAD_URL = '/b2api/v1/b2_get_upload_url'
LIST_BUCKETS = '/b2api/v1/b2_list_buckets'
CREATE_BUCKET = '/b2api/v1/b2_create_bucket'
LIST_FILE_NAMES = '/b2api/v1/b2_list_file_names'
DOWNLOAD_FILE_BY_ID = '/b2api/v1/b2_download_file_by_id'

KB = 1024
MB = KB ** 2
DEFAULT_CHUNKSIZE = 8 * MB


class B2AuthException(Exception):
    pass


class B2APIException(Exception):
    pass


class B2UploadException(Exception):
    pass


def sha1_file(fp):
    sha1 = hashlib.sha1()
    while True:
        data = fp.read(MB * 8)
        if not data:
            break
        sha1.update(data)
    return sha1.hexdigest()


class B2(object):
    account_id = None
    account_key = None
    auth_token = None
    auth_header = {}
    api_url = None
    download_url = None
    buckets = []

    def __init__(self, account_id, account_key):
        self.account_id = account_id
        self.account_key = account_key

    def authorize(self):
        r = requests.get(AUTHORIZE_ACCOUNT, auth=HTTPBasicAuth(self.account_id, self.account_key))
        if r.status_code == 200:
            rjs = r.json()
            self.auth_token = rjs['authorizationToken']
            self.auth_header = {'Authorization': self.auth_token}
            self.api_url = rjs['apiUrl']
            self.download_url = rjs['downloadUrl']
        else:
            raise B2AuthException('Could not authorize with B2')

    def _make_call(self, func, path, **kwargs):
        return func(self.api_url + path, **kwargs)

    def _get(self, path, **kwargs):
        return self._make_call(requests.get, path, **kwargs)

    def _post(self, path, **kwargs):
        return self._make_call(requests.post, path, **kwargs)

    def get_upload_url(self, bucket_id):
        data = json.dumps({'bucketId': bucket_id})
        r = self._post(GET_UPLOAD_URL, headers=self.auth_header, data=data)
        if r.status_code == 200:
            rjs = r.json()
            return rjs['uploadUrl'], rjs['authorizationToken']
        else:
            raise B2APIException('Could not get B2 upload url')

    def _upload(self, bucket_id, filename, filehash, data, content_type=None, headers=None):
        upload_url, auth_token = self.get_upload_url(bucket_id)
        base_headers = {
            'Authorization': auth_token,
            'X-Bz-File-Name': filename,
            'Content-Type': content_type or 'python-b2/x-auto',
            'X-Bz-Content-Sha1': filehash,
        }
        headers = {**base_headers, **(headers or {})}
        r = requests.post(upload_url, headers=headers, data=data)
        if r.status_code != 200:
            print(r.status_code)
            print(r.content)
            raise B2UploadException()
        return json.loads(r.content.decode())

    def _upload_data(self, bucket_id, filename, data):
        filehash = hashlib.sha1(data).hexdigest()
        return self._upload(bucket_id, filename, filehash, data)

    def _upload_file(self, bucket_id, filename, fp):
        filehash = sha1_file(fp)
        fp.seek(0, 0)
        return self._upload(bucket_id, filename, filehash, fp)

    def _upload_retry(self, func, *args, **kwargs):
        attempts = 5
        for i in range(attempts):
            try:
                return func(*args, **kwargs)
            except B2UploadException:
                pass
        return B2UploadException('Could not upload data to B2')

    def upload_data(self, bucket_id, filename, data):
        return self._upload_retry(self._upload_data, bucket_id, filename, data)

    def upload_file(self, bucket_id, filename, fp):
        return self._upload_retry(self._upload_file, bucket_id, filename, fp)
