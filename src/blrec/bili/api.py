import asyncio
import hashlib
from abc import ABC
from datetime import datetime
from typing import Any, Dict, Final, List, Mapping, Optional
from urllib.parse import urlencode, quote, parse_qs
import requests
import time
from functools import reduce

import aiohttp
from loguru import logger
from tenacity import retry, stop_after_delay, wait_exponential

from .exceptions import ApiRequestError
from .typing import JsonResponse, QualityNumber, ResponseData

__all__ = 'AppApi', 'WebApi'


BASE_HEADERS: Final = {
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en;q=0.3,en-US;q=0.2',  # noqa
    'Accept': 'application/json, text/plain, */*',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
    'Origin': 'https://live.bilibili.com',
    'Pragma': 'no-cache',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',  # noqa
}


class BaseApi(ABC):
    def __init__(
        self,
        session: aiohttp.ClientSession,
        headers: Optional[Dict[str, str]] = None,
        *,
        room_id: Optional[int] = None,
    ):
        self._logger = logger.bind(room_id=room_id or '')

        self.base_api_urls: List[str] = ['https://api.bilibili.com']
        self.base_live_api_urls: List[str] = ['https://api.live.bilibili.com']
        self.base_play_info_api_urls: List[str] = ['https://api.live.bilibili.com']

        self._session = session
        self.headers = headers or {}
        self.timeout = 10

    @property
    def headers(self) -> Dict[str, str]:
        return self._headers

    @headers.setter
    def headers(self, value: Dict[str, str]) -> None:
        self._headers = {**BASE_HEADERS, **value}

    @staticmethod
    def _check_response(json_res: JsonResponse) -> None:
        if json_res['code'] != 0:
            raise ApiRequestError(
                json_res['code'], json_res.get('message') or json_res.get('msg') or ''
            )

    @retry(reraise=True, stop=stop_after_delay(5), wait=wait_exponential(0.1))
    async def _get_json_res(self, *args: Any, **kwds: Any) -> JsonResponse:
        should_check_response = kwds.pop('check_response', True)
        kwds = {'timeout': self.timeout, 'headers': self.headers, **kwds}
        async with self._session.get(*args, **kwds) as res:
            self._logger.trace('Request: {}', res.request_info)
            self._logger.trace('Response: {}', await res.text())
            try:
                json_res = await res.json()
            except aiohttp.ContentTypeError:
                text_res = await res.text()
                self._logger.debug(f'Response text: {text_res[:200]}')
                raise
            if should_check_response:
                self._check_response(json_res)
            return json_res

    async def _get_json(
        self, base_urls: List[str], path: str, *args: Any, **kwds: Any
    ) -> JsonResponse:
        if not base_urls:
            raise ValueError('No base urls')
        exception = None
        for base_url in base_urls:
            url = base_url + path
            try:
                return await self._get_json_res(url, *args, **kwds)
            except Exception as exc:
                exception = exc
                self._logger.trace('Failed to get json from {}: {}', url, repr(exc))
        else:
            assert exception is not None
            raise exception

    async def _get_jsons_concurrently(
        self, base_urls: List[str], path: str, *args: Any, **kwds: Any
    ) -> List[JsonResponse]:
        if not base_urls:
            raise ValueError('No base urls')
        urls = [base_url + path for base_url in base_urls]
        aws = (self._get_json_res(url, *args, **kwds) for url in urls)
        results = await asyncio.gather(*aws, return_exceptions=True)
        exceptions = []
        json_responses = []
        for idx, item in enumerate(results):
            if isinstance(item, Exception):
                self._logger.trace(
                    'Failed to get json from {}: {}', urls[idx], repr(item)
                )
                exceptions.append(item)
            elif isinstance(item, dict):
                json_responses.append(item)
            else:
                self._logger.trace('{}', repr(item))
        if not json_responses:
            raise exceptions[0]
        return json_responses


class AppApi(BaseApi):
    # taken from https://github.com/SocialSisterYi/bilibili-API-collect/blob/master/other/API_sign.md  # noqa
    _appkey = '1d8b6e7d45233436'
    _appsec = '560c52ccd288fed045859ed18bffd973'

    _app_headers = {
        'User-Agent': 'Mozilla/5.0 BiliDroid/6.64.0 (bbcallen@gmail.com) os/android model/Unknown mobi_app/android build/6640400 channel/bili innerVer/6640400 osVer/6.0.1 network/2',  # noqa
        'Connection': 'Keep-Alive',
        'Accept-Encoding': 'gzip',
    }

    @property
    def headers(self) -> Dict[str, str]:
        return self._headers

    @headers.setter
    def headers(self, value: Dict[str, str]) -> None:
        self._headers = {**value, **self._app_headers}

    @classmethod
    def signed(cls, params: Mapping[str, Any]) -> Dict[str, Any]:
        if isinstance(params, Mapping):
            params = dict(sorted({**params, 'appkey': cls._appkey}.items()))
        else:
            raise ValueError(type(params))
        query = urlencode(params, doseq=True)
        sign = hashlib.md5((query + cls._appsec).encode()).hexdigest()
        params.update(sign=sign)
        return params

    async def get_room_play_infos(
        self,
        room_id: int,
        qn: QualityNumber = 10000,
        *,
        only_video: bool = False,
        only_audio: bool = False,
    ) -> List[ResponseData]:
        path = '/xlive/app-room/v2/index/getRoomPlayInfo'
        params = self.signed(
            {
                'actionKey': 'appkey',
                'build': '6640400',
                'channel': 'bili',
                'codec': '0,1',  # 0: avc, 1: hevc
                'device': 'android',
                'device_name': 'Unknown',
                'disable_rcmd': '0',
                'dolby': '1',
                'format': '0,1,2',  # 0: flv, 1: ts, 2: fmp4
                'free_type': '0',
                'http': '1',
                'mask': '0',
                'mobi_app': 'android',
                'need_hdr': '0',
                'no_playurl': '0',
                'only_audio': '1' if only_audio else '0',
                'only_video': '1' if only_video else '0',
                'platform': 'android',
                'play_type': '0',
                'protocol': '0,1',
                'qn': qn,
                'room_id': room_id,
                'ts': int(datetime.utcnow().timestamp()),
            }
        )
        json_responses = await self._get_jsons_concurrently(
            self.base_play_info_api_urls, path, params=params
        )
        return [r['data'] for r in json_responses]

    async def get_info_by_room(self, room_id: int) -> ResponseData:
        path = '/xlive/app-room/v1/index/getInfoByRoom'
        params = self.signed(
            {
                'actionKey': 'appkey',
                'build': '6640400',
                'channel': 'bili',
                'device': 'android',
                'mobi_app': 'android',
                'platform': 'android',
                'room_id': room_id,
                'ts': int(datetime.utcnow().timestamp()),
            }
        )
        json_res = await self._get_json(self.base_live_api_urls, path, params=params)
        return json_res['data']

    async def get_user_info(self, uid: int) -> ResponseData:
        base_api_urls = ['https://app.bilibili.com']
        path = '/x/v2/space'
        params = self.signed(
            {
                'build': '6640400',
                'channel': 'bili',
                'mobi_app': 'android',
                'platform': 'android',
                'ts': int(datetime.utcnow().timestamp()),
                'vmid': uid,
            }
        )
        json_res = await self._get_json(base_api_urls, path, params=params)
        return json_res['data']

    async def get_danmu_info(self, room_id: int) -> ResponseData:
        path = '/xlive/app-room/v1/index/getDanmuInfo'
        params = self.signed(
            {
                'actionKey': 'appkey',
                'build': '6640400',
                'channel': 'bili',
                'device': 'android',
                'mobi_app': 'android',
                'platform': 'android',
                'room_id': room_id,
                'ts': int(datetime.utcnow().timestamp()),
            }
        )
        json_res = await self._get_json(self.base_live_api_urls, path, params=params)
        return json_res['data']


class WebApi(BaseApi):
    async def room_init(self, room_id: int) -> ResponseData:
        path = '/room/v1/Room/room_init'
        params = {'id': room_id}
        json_res = await self._get_json(self.base_live_api_urls, path, params=params)
        return json_res['data']

    async def get_room_play_infos(
        self, room_id: int, qn: QualityNumber = 10000
    ) -> List[ResponseData]:
        path = '/xlive/web-room/v2/index/getRoomPlayInfo'
        params = {
            'room_id': room_id,
            'protocol': '0,1',
            'format': '0,1,2',
            'codec': '0,1',
            'qn': qn,
            'platform': 'web',
            'ptype': 8,
        }
        json_responses = await self._get_jsons_concurrently(
            self.base_play_info_api_urls, path, params=params
        )
        return [r['data'] for r in json_responses]

    async def get_info_by_room(self, room_id: int) -> ResponseData:
        path = '/xlive/web-room/v1/index/getInfoByRoom'
        params = {'room_id': room_id}
        json_res = await self._get_json(self.base_live_api_urls, path, params=params)
        return json_res['data']

    async def get_info(self, room_id: int) -> ResponseData:
        path = '/room/v1/Room/get_info'
        params = {'room_id': room_id}
        json_res = await self._get_json(self.base_live_api_urls, path, params=params)
        return json_res['data']

    async def get_timestamp(self) -> int:
        path = '/av/v1/Time/getTimestamp'
        params = {'platform': 'pc'}
        json_res = await self._get_json(self.base_live_api_urls, path, params=params)
        return json_res['data']['timestamp']

    async def get_user_info(self, uid: int) -> ResponseData:
        # FIXME: "code": -352, "message": "风控校验失败",
        path = '/x/space/wbi/acc/info'
        params = {'mid': uid}
        json_res = await self._get_json(self.base_api_urls, path, params=params)
        return json_res['data']

    async def get_danmu_info(self, room_id: int) -> ResponseData:
        cookie = self.headers.get('Cookie', '')
        path = '/xlive/web-room/v1/index/getDanmuInfo'
        params = {
            'id': room_id,
            "type": 0,
            }
        w_rid, wts = wbi_sign_params(params.copy(), cookie)
        params["w_rid"] = w_rid
        params["wts"] = wts
        if cookie != '' and 'buvid3' not in cookie:
            buvid3, buvid4 = get_buvid(cookie)
            self.headers['Cookie'] += f'; buvid3={buvid3}'

        json_res = await self._get_json(self.base_live_api_urls, path, params=params)
        return json_res['data']


    async def get_nav(self) -> ResponseData:
        path = '/x/web-interface/nav'
        json_res = await self._get_json(self.base_api_urls, path, check_response=False)
        return json_res
    
    
    
    
mixinKeyEncTab = [
    46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35, 27, 43, 5, 49,
    33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13, 37, 48, 7, 16, 24, 55, 40,
    61, 26, 17, 0, 1, 60, 51, 30, 4, 22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11,
    36, 20, 34, 44, 52
]

def getMixinKey(orig: str):
    '对 imgKey 和 subKey 进行字符顺序打乱编码'
    return reduce(lambda s, i: s + orig[i], mixinKeyEncTab, '')[:32]

def encWbi(params: dict, img_key: str, sub_key: str):
    '为请求参数进行 wbi 签名'
    mixin_key = getMixinKey(img_key + sub_key)
    curr_time = round(time.time())
    params['wts'] = curr_time                                   # 添加 wts 字段
    params = dict(sorted(params.items()))                       # 按照 key 重排参数
    # 过滤 value 中的 "!'()*" 字符
    params = {
        k : ''.join(filter(lambda chr: chr not in "!'()*", str(v)))
        for k, v 
        in params.items()
    }
    query = urlencode(params)                      # 序列化参数
    wbi_sign = hashlib.md5((query + mixin_key).encode()).hexdigest()    # 计算 w_rid
    params['w_rid'] = wbi_sign
    return params

def getWbiKeys(cookie: str) -> tuple[str, str]:
    '获取最新的 img_key 和 sub_key'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Referer': 'https://www.bilibili.com/',
        'Cookie': cookie,  # 使用传入的 cookie
    }
    resp = requests.get('https://api.bilibili.com/x/web-interface/nav', headers=headers)
    resp.raise_for_status()
    json_content = resp.json()
    img_url: str = json_content['data']['wbi_img']['img_url']
    sub_url: str = json_content['data']['wbi_img']['sub_url']
    img_key = img_url.rsplit('/', 1)[1].split('.')[0]
    sub_key = sub_url.rsplit('/', 1)[1].split('.')[0]
    return img_key, sub_key

def wbi_sign_params(params: dict, cookie: str) -> str:
    """
    获取 WBI Keys 并对参数进行签名
    """
    img_key, sub_key = getWbiKeys(cookie)
    signed_params = encWbi(params, img_key, sub_key)
    return signed_params["w_rid"], signed_params["wts"]


def get_buvid(cookie: str) -> str:
    """
    从 https://api.bilibili.com/x/frontend/finger/spi 中获取 buvid3
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'Referer': 'https://www.bilibili.com/',
        'Cookie': cookie,  # 使用传入的 cookie
    }
    resp = requests.get('https://api.bilibili.com/x/frontend/finger/spi', headers=headers)
    resp.raise_for_status()
    json_content = resp.json()
    if json_content['code'] != 0:
        raise ValueError("Failed to retrieve buvid3")
    buvid3 = json_content['data']['b_3']
    buvid4 = json_content['data'].get('b_4')
    if not buvid3:
        raise ValueError("Response does not contain 'buvid3'")
    if not buvid4:
        raise ValueError("Response does not contain 'buvid4'")
    return buvid3, buvid4