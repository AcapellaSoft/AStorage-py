import asyncio
from asyncio import AbstractEventLoop
from typing import List, Optional, Iterable
from urllib.parse import quote

from aiohttp import ClientSession, ClientResponse

from acapella.kv.utils.errors import CasError, TransactionNotFoundError, TransactionCompletedError, KvError, \
    AuthenticationFailedError


def key_to_str(key: Iterable[str]) -> str:
    return ':'.join(quote(part) for part in key)


def entry_url(partition: List[str], clustering: Optional[List[str]] = None) -> str:
    if clustering is None or len(clustering) == 0:
        return f'/astorage/v2/kv/keys/{key_to_str(partition)}'
    return f'/astorage/v2/kv/partition/{key_to_str(partition)}/clustering/{key_to_str(clustering)}'


def raise_if_error(code: int):
    if code == 200:
        return
    if code == 401:
        raise AuthenticationFailedError()
    if code == 408:
        raise TimeoutError()
    if code == 409:
        raise CasError()
    if code == 410:
        raise TransactionNotFoundError()
    if code == 412:
        raise TransactionCompletedError()
    raise KvError(f'Unexpected server error with code {code}')


class AsyncSession(object):
    def __init__(self, session: ClientSession = None, loop: AbstractEventLoop = None, base_url: str = ''):
        self._session = session or ClientSession()
        self._loop = loop or asyncio.get_event_loop()
        self._base_url = base_url
        self._auth = None

    async def _request(self, method, url, **kwargs):
        kwargs = {'auth': self._auth, **kwargs}
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        return await self._session.request(method, self._base_url + url, **kwargs)

    async def get(self, url, **kwargs) -> ClientResponse:
        return await self._request('get', url, **kwargs)

    async def options(self, url, **kwargs) -> ClientResponse:
        return await self._request('options', url, **kwargs)

    async def head(self, url, **kwargs) -> ClientResponse:
        return await self._request('head', url, **kwargs)

    async def post(self, url, data=None, json=None, **kwargs) -> ClientResponse:
        return await self._request('post', url, data=data, json=json, **kwargs)

    async def put(self, url, data=None, **kwargs) -> ClientResponse:
        return await self._request('put', url, data=data, **kwargs)

    async def patch(self, url, data=None, **kwargs) -> ClientResponse:
        return await self._request('patch', url, data=data, **kwargs)

    async def delete(self, url, **kwargs) -> ClientResponse:
        return await self._request('delete', url, **kwargs)

    def set_cookie(self, cookies):
        self._session.cookie_jar.update_cookies(cookies)

    def set_auth(self, auth):
        self._auth = auth
