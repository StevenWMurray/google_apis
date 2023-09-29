"""Module for requesting authenticated service objects"""
import pickle
import json
import yaml
import time
import random
from enum import Enum
from pathlib import PosixPath
from functools import cache, cached_property, wraps
from collections.abc import Iterable
from typing import Optional, NewType, TYPE_CHECKING, cast, ClassVar, Callable, \
    TypeVar, NamedTuple, Any
from warnings import warn

from attrs import define, field, frozen
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

if TYPE_CHECKING:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request


__all__ = ('Services', 'send_request')

GOOGLE_ADS_API_VERSION = 'v14'
RefreshToken = NewType('RefreshToken', str)
T = TypeVar('T')
default_scopes = {
    'https://www.googleapis.com/auth/adwords',
    'https://www.googleapis.com/auth/analytics',
    'https://www.googleapis.com/auth/analytics.edit',
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/tagmanager.edit.containers',
    'https://www.googleapis.com/auth/tagmanager.edit.containerversions',
    'https://www.googleapis.com/auth/tagmanager.manage.users',
    'https://www.googleapis.com/auth/content'
}


class ApiDataTuple(NamedTuple):
    api_name: str
    version: str


class DiscoveryServices:
    UaReporting = ApiDataTuple('analyticsreporting', 'v4')
    UaManagement = ApiDataTuple('analytics', 'v3')
    Sheets = ApiDataTuple('sheets', 'v4')
    TagManager = ApiDataTuple('tagmanager', 'v2')
    MerchantCenter = ApiDataTuple('content', 'v2.1')


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


def attempt(
    fn: Callable[..., T],
    recoverableErrors: tuple[Exception, ...],
    fallback: Callable[..., T]
) -> Callable[..., T]:
    @wraps(fn)
    def inner(*args, **kwargs) -> T:
        try:
            return fn(*args, **kwargs)
        except tuple(recoverableErrors): # type: ignore[misc]
            return fallback(*args, **kwargs)

    return inner


@frozen
class Context:
    owner: str
    email: str
    client_secret_path: ClientSecret = field(
        converter=ClientSecret, default=ClientSecret.home())
    token_path: PosixPath = field(converter=PosixPath, default=PosixPath.home())
    ads_auth_path: GoogleAdsToken = field(
        converter=GoogleAdsToken, default=GoogleAdsToken.home())

    @classmethod
    def from_json(cls: type['Context'], ddict: dict) -> 'Context':
        """Instantiate the context object from a dictionary.

        BUGGED: PosixPath / None returns a Type Error
        """
        return cls(
            ddict['owner'],
            ddict['data'].get('email'),
            PosixPath.home() / ddict['data'].get('client_secret_path', None),
            PosixPath.home() / ddict['data'].get('token_path', None),
            PosixPath.home() / ddict['data'].get('google_ads_yaml_path', None))


@define
class AuthRoot:
    """Storage container for WD account scoped auth info

    This class handles auth token management for any given WD account.
    It requires the following file paths:
        clientSecrets: the JSON file from Google Cloud Console
        token: the path to the pickled credentials. If they don't exist
            yet, name the path where they should be stored once created.
        adsAuth: the YAML file downloaded from Google Ads

    Currently up for rewrite. The current design is too tightly coupled to a
    specific means of API key storage. An updated design will allow keys to be
    stored as environment variables as well.

    Desired changes:
        - Allow user to define which token storage mechanisms to use
    """
    wd_acct_name: str
    context: Context
    scopes: set[str] = field(default=default_scopes)
    credential_store: 'Credentials' = field(init=False)

    def write_refresh_token(self, refresh_token: RefreshToken) -> None:
        """Posts the refresh token to all auth token files"""
        self.context.client_secret_path.write_refresh(refresh_token)
        if self.context.ads_auth_path is not None:
            self.context.ads_auth_path.write_refresh(refresh_token)

    def credentials(
        self,
        force_refresh: bool = False
    ) -> 'Credentials':
        """Returns the oauth2 credentials associated with the provided tokens

        Checks the following places (in order):
            - Valid Credentials object already exists on self
            - Valid pickled credentials object
            - Expired pickled credentials that can be refreshed
            - Build credentials from client secret file data
        """
        if force_refresh:
            self.build_credentials()
            self.context.token_path.write_bytes(pickle.dumps(self.credential_store))
        elif not self.credentials_valid:
            try:
                self.credential_store = pickle.loads(self.context.token_path.read_bytes())
                assert self.credentials_valid
            except (AssertionError, AttributeError):
                try:
                    self.try_refresh()
                except (AttributeError, AssertionError, RefreshError):
                    self.build_credentials()
                finally:
                    self.context.token_path.write_bytes(pickle.dumps(self.credential_store))
            except (FileNotFoundError, RefreshError):
                self.build_credentials()
                self.context.token_path.write_bytes(pickle.dumps(self.credential_store))
        return self.credential_store

    def try_refresh(self) -> None:
        """Attempts to refresh the auth token"""
        assert self.credentials_refreshable

        # lazy load expensive module
        from google.auth.transport.requests import Request
        self.credential_store.refresh(Request())

    def build_credentials(self) -> None:
        """(Re-)Build credentials from client secrets file"""
        # Lazy load expensive module
        from google_auth_oauthlib.flow import InstalledAppFlow
        flow = InstalledAppFlow.from_client_secrets_file(
            self.context.client_secret_path, scopes=list(self.scopes))
        self.credential_store = flow.run_local_server(port=0)
        assert self.credentials_valid
        self.write_refresh_token(
            cast(RefreshToken, self.credential_store.refresh_token))

    @property
    def credentials_valid(self):
        """Check credentials for validity and sufficient scope"""
        return hasattr(self, 'credential_store') and \
            self.credential_store.valid and \
            self.scopes.issubset(self.credential_store.scopes)

    @property
    def credentials_refreshable(self):
        """Check whether invalid credentials can be refreshed"""
        return hasattr(self, 'credential_store') and \
            self.credential_store.expired and \
            self.credential_store.refresh_token and \
            self.scopes.issubset(self.credential_store.scopes)

    def check_scopes(self, scopes: set[str]) -> bool:
        return True

    def upgrade_scopes(self, scopes: set[str]) -> None:
        """Update credentials to include requested scopes

        This will not remove any existing scopes on the Auth Root object. Spawn
        a new Auth Root to request a credentials downgrade.
        """


def first(condition: Callable[[T], bool], iterable: Iterable[T]) -> T:
    """Returns the first item in an iterable matching the condition

    If the condition is not satisfied by any item in the iterable, it raises
    a StopIteration error.
    """
    return next(filter(condition, iterable))


class Services:
    """API for requesting Google Services.

    This class accepts an AuthRoot object, and uses it to request auth tokens
    for the provided credentials. Those tokens are then bound to API service
    objects to make authenticated requests to various Google APIs.
    """
    # Flyweight pattern to cache authorized sessions on a per-account level
    contexts: ClassVar[dict[str, 'Services']] = {}

    def __init__(
        self,
        auth_root: AuthRoot,
        context_owner: Optional[str] = None,
        force_refresh: bool = False
    ) -> None:
        self._creds = auth_root.credentials(force_refresh=force_refresh)
        self._ads_path = auth_root.context.ads_auth_path
        if context_owner is None:
            context_owner = auth_root.wd_acct_name
        self.__class__.contexts[context_owner] = self
        self.services: dict[ApiDataTuple, Any] = {}

    @classmethod
    def from_auth_context(
        cls,
        context_owner: str,
        context_file_path: PosixPath =
            PosixPath.home().joinpath('.auth/auth_config.json'),
        scopes: Optional[set[str]] = None
    ) -> 'Services':
        """Read the auth token paths in from a json config file"""

        def owner_eq(context: dict[str, str]) -> bool:
            return context['owner'] == context_owner

        # just return the existing credentials if they already exist
        if context_owner in cls.contexts:
            return cls.contexts[context_owner]
        context_file = PosixPath(context_file_path)
        context_data: list[dict] = json.loads(context_file.read_text())

        try:
            context = Context.from_json(first(owner_eq, context_data))
        except StopIteration:
            raise KeyError(
                f"Couldn't find context owner {context_owner} in context file")

        aroot = AuthRoot(context.email, context, scopes or default_scopes)
        return cls(aroot, context_owner)

    @cache
    def ads_client(self, version: str = GOOGLE_ADS_API_VERSION):
        """Returns top level Google Ads API interface"""
        import google.ads.googleads.client as ads
        return ads.GoogleAdsClient.load_from_storage(self._ads_path, version=version)

    @cache
    def ads_service(
        self,
        service_name: str = 'GoogleAdsService',
        version: str = GOOGLE_ADS_API_VERSION
    ):
        """Returns Google Ads performance reporting interface"""
        return self.ads_client(version=version).get_service(service_name, version=version)

    @cached_property
    def ads_customer_service(self):
        """Returns Google Ads customer service"""
        return self.ads_client.get_service(
            'CustomerService',
            version=GOOGLE_ADS_API_VERSION)

    def discovery_service(self, api: ApiDataTuple, version: Optional[str] = None):
        """Returns authenticated service object for Discovery Document API"""
        if version is not None and version != api.version:
            api = ApiDataTuple(api.api_name, version)
        if api not in self.services:
            service = build(api.api_name, api.version, credentials=self._creds)
            self.services[api] = service
        else:
            service = self.services[api]
        return service

    @cache
    def ga4_service(self, scope='rw'):
        from google.analytics.data_v1beta import BetaAnalyticsDataClient
        return BetaAnalyticsDataClient(credentials=self._creds)

    @cache
    def ga4_admin_service(self, version='v1_beta', scope='rw'):
        if version == 'v1_beta':
            from google.analytics.admin_v1beta import AnalyticsAdminServiceClient
        if version == 'v1_alpha':
            from google.analytics.admin_v1alpha import AnalyticsAdminServiceClient
        return AnalyticsAdminServiceClient(credentials=self._creds)

    @property
    def sheets_service(self):
        """Returns authenticated service object for Google Sheets API"""
        return self.discovery_service(DiscoveryServices.Sheets)

    @property
    def analytics_service(self):
        """Returns authenticated service object for GA reporting API"""
        return self.discovery_service(DiscoveryServices.UaReporting)

    @property
    def analytics_management_service(self):
        """Returns authenticated service object for GA account management API"""
        return self.discovery_service(DiscoveryServices.UaManagement)

    @property
    def tagmanager_service(self):
        """Returns authenticated service object for GTM API"""
        return self.discovery_service(DiscoveryServices.TagManager)

    @property
    def merchant_center_service(self):
        """Returns authenticated service object for Google Content API"""
        return self.discovery_service(DiscoveryServices.MerchantCenter)


def send_request(request):
    """Make API requests with exponential backoff"""
    retryable_errors = (
        'userRateLimitExceeded',
        'quotedExceeded',
        'internalServerError',
        'backendError',
        'rateLimitExceeded',
        'Too Many Requests')

    max_retries = 6
    for n in range(0, max_retries):
        try:
            return request.execute()

        except HttpError as error:
            if error.resp.reason in retryable_errors and n < max_retries:
                time.sleep((2 ** n) + random.random())
            else:
                raise error


if __name__ == "__main__":
    """Basic integration test suite

    This is kept separate from the main unit tests because everything here has
    to call either the filesystem or an external API.
    """
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow

    # Test service setup
    context_key = 'GoogleAds'
    serv = Services.from_auth_context(context_key)
    serv.ads_client()
    serv.ads_service()
    serv.ga4_service()
    serv.ga4_admin_service(version='v1_beta')
    serv.ga4_admin_service(version='v1_alpha')
    serv.sheets_service
    serv.analytics_service
    serv.analytics_management_service
    serv.tagmanager_service
    serv.merchant_center_service

    # Test flyweight caching
    serv2 = Services.from_auth_context(context_key)
    serv2.discovery_service(DiscoveryServices.UaReporting, version='v4')
    assert(serv2 is serv) # Same context key
    assert(serv2.analytics_service is serv.analytics_service)

    serv3 = Services.from_auth_context('StevenMurray')
    assert(serv3 is not serv) # Different context key
    assert(serv3.sheets_service is not serv.sheets_service)
    print("All tests passed!")
