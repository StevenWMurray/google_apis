"""Module for requesting authenticated service objects"""
import pickle
import json
import yaml
from enum import Enum
from pathlib import PosixPath
from dataclasses import dataclass
from typing import Optional, NewType
from functools import cached_property, lru_cache

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

GOOGLE_ADS_API_VERSION = 'v7'

RefreshToken = NewType('RefreshToken', str)
default_scopes = [
    'https://www.googleapis.com/auth/analytics',
    'https://www.googleapis.com/auth/analytics.edit',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/tagmanager.edit.containers',
    'https://www.googleapis.com/auth/tagmanager.edit.containerversions',
    'https://www.googleapis.com/auth/tagmanager.manage.users'
]

class ClientSecret(PosixPath):
    """Binds standard auth token I/O methods to the auth token's Path"""
    def write_refresh(self, refresh_token: RefreshToken) -> None:
        """Add / Overwrite refresh token in token file"""
        with self.open('r') as f:
            data = json.load(f)

        with self.open('w') as f:
            data['installed']['refresh_token'] = refresh_token
            json.dump(data, f)


class GoogleAdsToken(PosixPath):
    """Binds standard auth token I/O methods to the auth token's Path"""
    def write_refresh(self, refresh_token: RefreshToken) -> None:
        """Add / Overwrite refresh token in token file"""
        with self.open('r') as f:
            data = yaml.load(f, Loader=yaml.SafeLoader)

        with self.open('w') as f:
            data['refresh_token'] = refresh_token
            yaml.dump(data, f)


@dataclass
class AuthRoot:
    """Storage container for WD account scoped auth info

    This class handles auth token management for any given WD account.
    It requires the following file paths:
        clientSecrets: the JSON file from Google Cloud Console
        token: the path to the pickled credentials. If they don't exist
            yet, name the path where they should be stored once created.
        adsAuth: the YAML file downloaded from Google Ads
    """
    wdAcctName: str
    clientSecretPath: ClientSecret
    tokenPath: PosixPath
    adsAuthPath: Optional[GoogleAdsToken] = None
    force_refresh: bool = False

    def __post_init__(self) -> None:
        self._credentials = None
        self._default_scopes = [
            'https://www.googleapis.com/auth/adwords',
            'https://www.googleapis.com/auth/analytics',
            'https://www.googleapis.com/auth/analytics.edit',
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/tagmanager.edit.containers',
            'https://www.googleapis.com/auth/tagmanager.edit.containerversions',
            'https://www.googleapis.com/auth/tagmanager.manage.users'
        ]

    def write_refresh_token(self, refresh_token: RefreshToken) -> None:
        """Posts the refresh token to all auth token files"""
        self.clientSecretPath.write_refresh(refresh_token)
        if self.adsAuthPath is not None:
            self.adsAuthPath.write_refresh(refresh_token)

    @cached_property
    def credentials(self) -> Credentials:
        """Returns the oauth2 credentials associated with the provided tokens

        Checks the following places (in order):
            - Credentials object already exists on self
            - Valid pickled credentials object
            - Expired pickled credentials that can be refreshed
            - Build credentials from client secret file data
        """
        if self.force_refresh:
            self.build_credentials()
            self.tokenPath.write_bytes(pickle.dumps(self._credentials))
        elif self._credentials is None:
            try:
                self._credentials = pickle.loads(self.tokenPath.read_bytes())
                assert self._credentials.valid
            except (AssertionError, AttributeError):
                try:
                    self.try_refresh()
                except (AttributeError, AssertionError):
                    self.build_credentials()
                finally:
                    self.tokenPath.write_bytes(pickle.dumps(self._credentials))
            except FileNotFoundError:
                self.build_credentials()
                self.tokenPath.write_bytes(pickle.dumps(self._credentials))
        return self._credentials

    def try_refresh(self) -> None:
        """Attempts to refresh the auth token"""
        assert self._credentials.expired and self._credentials.refresh_token

        # lazy load expensive module
        from google.auth.transport.requests import Request
        self._credentials.refresh(Request())

    def build_credentials(self) -> None:
        """(Re-)Build credentials from client secrets file"""
        # Lazy load expensive module
        from google_auth_oauthlib.flow import InstalledAppFlow
        flow = InstalledAppFlow.from_client_secrets_file(
            self.clientSecretPath, scopes=self._default_scopes)
        self._credentials = flow.run_local_server(port=0)
        self.write_refresh_token(self._credentials.refresh_token)


def index_of(iterable, condition):
    """Returns the first item in an iterable matching the condition

    If the condition is not satisfied by any item in the iterable, it raises
    a StopIteration error.
    """
    return next(x for x in iterable if condition(x))

class Services:
    """API for requesting Google Services.

    This class accepts an AuthRoot object, and uses it to request auth tokens
    for the provided credentials. Those tokens are then bound to API service
    objects to make authenticated requests to various Google APIs.
    """
    # Flyweight pattern to cache authorized sessions on a per-account level
    contexts = {}

    def __init__(
        self,
        auth_root: AuthRoot,
        context_owner: Optional[str] = None,
    ) -> None:
        self._creds = auth_root.credentials
        self._ads_path = auth_root.adsAuthPath
        if context_owner is None:
            context_owner = auth_root.wdAcctName
        self.__class__.contexts[context_owner] = self

    @classmethod
    def from_auth_context(
        cls,
        context_owner: str,
        context_file_path: PosixPath =
            PosixPath.home().joinpath('.auth/auth_config.json')
    ):
        """Read the auth token paths in from a json config file"""
        if context_owner in cls.contexts:
            return cls.contexts[context_owner]
        context_file = PosixPath(context_file_path)
        context_data = json.loads(context_file.read_text())
        owner_eq = lambda context: context['owner'] == context_owner
        context = index_of(context_data, owner_eq)['data']
        aroot = AuthRoot(
            context['email'],
            ClientSecret(PosixPath.home() / context['client_secret_path']),
            PosixPath.home() / PosixPath(context['token_path']),
            GoogleAdsToken(PosixPath.home() / context['google_ads_yaml_path']))
        return cls(aroot, context_owner)

    @cached_property
    def ads_client(self):
        """Returns top level Google Ads API interface"""
        import google.ads.googleads as ads
        return ads.client.GoogleAdsClient.load_from_storage(self._ads_path)

    @cached_property
    def ads_service(self):
        """Returns Google Ads performance reporting interface"""
        return self.ads_client.get_service(
            'GoogleAdsService', 
            version=GOOGLE_ADS_API_VERSION)

    @cached_property
    def ads_customer_service(self):
        """Returns Google Ads customer service"""
        return self.ads_client.get_service(
            'CustomerService',
            version=GOOGLE_ADS_API_VERSION)

    @lru_cache
    def serialize_enum(self, enum_class_name: str) -> Enum:
        """Fetch the requested enum from the ads client.

        Not really required anymore with recent changes to the Google Ads client
        library, but retained for backwards compatibility with existing code.
        """
        return getattr(self.ads_client.enums, enum_class_name + 'Enum')

    @cached_property
    def sheets_service(self):
        """Returns authenticated service object for Google Sheets API"""
        return build('sheets', 'v4', credentials=self._creds)

    @cached_property
    def analytics_service(self):
        """Returns authenticated service object for GA reporting API"""
        return build('analyticsreporting', 'v4', credentials=self._creds)

    @cached_property
    def analytics_management_service(self):
        """Returns authenticated service object for GA account management API"""
        return build('analytics', 'v3', credentials=self._creds)

    @cached_property
    def tagmanager_service(self):
        """Returns authenticated service object for GTM API"""
        return build('tagmanager', 'v2', credentials=self._creds)


if __name__ == "__main__":
    """Basic test suite

    This is kept separate from the main unit tests because everything here has
    to call either the filesystem or an external API.
    """
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow

    # Test service setup
    context_key = 'GoogleAds'
    serv = Services.from_auth_context(context_key)
    serv.ads_client
    serv.ads_service
    ctype = serv.serialize_enum('ClickType')
    serv.sheets_service
    serv.analytics_service
    serv.analytics_management_service
    serv.tagmanager_service

    # Test flyweight caching
    serv2 = Services.from_auth_context(context_key)
    serv2.analytics_service
    ctype2 = serv2.serialize_enum('ClickType')
    assert(serv2 is serv) # Same context key
    assert(serv2.analytics_service is serv.analytics_service)
    assert(ctype2 is ctype)

#     serv3 = Services.from_auth_context('GoogleAds')
#     assert(serv3 is not serv) # Different context key
    print("All tests passed!")
