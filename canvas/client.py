import requests
from requests.adapters import HTTPAdapter, Retry

# from ayx_plugins.utilities import format_url
# from .auth import authenticate


def format_url(url):
    """Ensure the URL ends with a trailing slash."""
    if not url.endswith('/'):
        return url + '/'
    return url


class APIClient:
    def __init__(self, connection_info):
        self.session = requests.Session()
        retries = Retry(
            total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        self.host_name_url = format_url(connection_info['dataSource']['parameters']['hostName'])
        self.account_id = connection_info['dataSource']['parameters']['accountId']
        self.token = connection_info['credentials']['bearerToken']['secrets']['bearerToken']['value']


