from gevent import monkey
monkey.patch_all()

import collections

import gevent
from gevent.lock import BoundedSemaphore
from gevent import pool

import urllib3
urllib3.disable_warnings()

import requests

from timeit import default_timer
from parsel import Selector as _ParselSelector

from typing import Callable, Dict, TypeVar
IntOrFloat = TypeVar([int, float])

import logging
logger = logging.getLogger(__name__)
_logger_handler = logging.StreamHandler()
_logger_formatter = logging.Formatter(
        '[%(levelname)s] %(asctime)s - %(message)s')
_logger_handler.setFormatter(_logger_formatter)
logger.addHandler(_logger_handler)
logger.setLevel("INFO")

class Request:
    def __init__(self, url: str, callback: Callable=None, method="GET", save: Dict=None, **kwargs):
        req_args = {k: v for k, v in kwargs.items() if k in ("method", "url", "headers", "files", "data",
                                                     "params", "auth", "cookies", "hooks", "json")}
        self.send_args = {k: v for k, v in kwargs.items() if k in ("allow_redirects",)}
        self._raw_req_obj = requests.Request(url=url, method=method, **req_args)
        self._raw_prepared_req_obj = self._raw_req_obj.prepare()
        self.callback = callback
        self._save = dict(save) if save is not None else {}

    @property
    def save(self):
        return self._save

    @save.setter
    def save(self, value):
        self._save = dict(value)

    def __getattr__(self, item):
        if item not in self.__dict__:
            return getattr(self._raw_req_obj, item)

class Response:
    def __init__(self, request: Request, raw_resp_obj: requests.Response):
        self._raw_resp_obj = raw_resp_obj
        self.request = request
        self.error = None
        self._save = {}

        self.selector = Selector(response=self._raw_resp_obj)

    def __getattr__(self, item):
        if item not in self.__dict__:
            return getattr(self._raw_resp_obj, item)

    @property
    def encoding(self):
        return self._raw_resp_obj.encoding

    @encoding.setter
    def encoding(self, value):
        self._raw_resp_obj.encoding = value

    def detect_charset(self):
        raise NotImplementedError

    def xpath(self, *args, **kwargs):
        return self.selector.xpath(*args, **kwargs)

    def css(self, *args, **kwargs):
        return self.selector.css(*args, **kwargs)

    def re(self, *args, **kwargs):
        return self.selector.re(*args, **kwargs)

    def re_first(self, *args, **kwargs):
        return self.selector.re_first(*args, **kwargs)

    @property
    def save(self):
        return self._save

    @save.setter
    def save(self, value):
        self._save = dict(value)


class Project:
    def __init__(self,
                 concurrent: int=2,
                 interval: IntOrFloat=1,
                 max_retry: int=5,
                 process_timeout: IntOrFloat=30,
                 request_timeout: IntOrFloat=60,
                 retry_delay:IntOrFloat=60,
                 *args, **kwargs):
        self.concurrent = concurrent
        self.interval = interval
        self.max_retry = max_retry
        self.process_timeout = process_timeout
        self.request_timeout = request_timeout
        self.retry_delay = retry_delay

    def report_sentry(self):
        raise NotImplementedError

    def start_requests(self):
        raise NotImplementedError

    def parse(self, resp: Response):
        raise NotImplementedError

    def result_handler(self, rv):
        return rv


class Selector(_ParselSelector):
    __slots__ = ('response',)

    def __init__(self, response=None, text=None, root=None, **kwargs):
        if not(response is None or text is None):
           raise ValueError('%s.__init__() received both response and text'
                            % self.__class__.__name__)

        if response is not None:
            text = response.text
            kwargs.setdefault('base_url', response.url)

        self.response = response
        super(Selector, self).__init__(text=text, root=root, **kwargs)


class BiuCore:
    def __init__(self, project: Project):
        self.project = project
        self._pool = pool.Pool()
        self._semaphore = BoundedSemaphore(project.concurrent)
        self._last_time = 0
        self._interval = project.interval
        self._max_retry = project.max_retry
        self._default_process_timeout = project.process_timeout
        self._default_request_timeout = project.request_timeout
        self._session = requests.Session()

        self._session.verify = False
        self._retry_delay = project.retry_delay

    def send_request(self, req: Request):
        raw_req = req._raw_req_obj.prepare()
        retried = 0
        for i in range(50):
            retried = i
            try:
                logger.debug('Retried {}'.format(retried))
                logger.debug('Request sent {}'.format(raw_req.url))

                send_args = req.send_args
                raw_resp = self.rate_limit_send_request(raw_req, send_args)
                raw_resp.raise_for_status()
                ## todo: handle error code
                resp = Response(req, raw_resp)
                return self._pool.spawn(self.callback_handler, req, resp)
            except requests.Timeout as e:
                logger.error("Fetch timeout!")
                if retried < self._max_retry:
                    gevent.sleep(self._retry_delay)
                    continue
                return
            except Exception as e:
                logger.error(e)
                if retried < self._max_retry:
                    gevent.sleep(self._retry_delay)
                    continue
                return
        logger.error('Retry failed! {} {}'.format(raw_req.url, retried))

    def rate_limit_send_request(self, req: requests.PreparedRequest, send_args: dict=None, proxies: dict=None):
        gevent.sleep(0)
        with self._semaphore:
            last, current = self._last_time, default_timer()
            elapsed = current - last
            if elapsed < self._interval:
                gevent.sleep(self._interval - elapsed)
            self._last_time = default_timer()

            return self._session.send(req, verify=False, proxies=proxies,
                                      timeout=self._default_request_timeout, **send_args)

    def callback_handler(self, req: Request, resp: Response, pre_resp: Response=None):
        logger.info("Request: %s %s %s", resp.status_code, req.method, resp.url)
        resp.save = dict(req.save)
        if not resp.save:
            resp.save = {}
            if pre_resp:
                resp.save.update(pre_resp.save)
        handler = req.callback
        try:
            with gevent.Timeout(self._default_process_timeout):
                processed_rv = handler(resp)
                self.process_value(processed_rv, pre_resp=resp)
        except gevent.Timeout:
            logger.error("Processing timeout")
            return
        except Exception as e:
            logger.error("Exception %s", e)
            raise

    def errorback_handler(self, resp: Response):
        return

    def result_handler(self, result):
        return

    def process_value(self, rv, pre_resp: Response=None):
        if isinstance(rv, dict):
            return self.result_handler(self.project.result_handler(rv))
        elif isinstance(rv, Request):
            if pre_resp:
                new_save = dict(pre_resp.save)
                new_save.update(rv.save)
                rv.save = new_save
            return self.send_request(rv)
        elif isinstance(rv, collections.Iterable):
            for i in rv:
                self.process_value(i, pre_resp=pre_resp)

    def run(self):
        self.process_value(self.project.start_requests())
        self._pool.join()


def run(proj_obj: Project):
    BiuCore(proj_obj).run()