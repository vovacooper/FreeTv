import urlparse
import functools
from flask import request, redirect, Response
from werkzeug.contrib.cache import RedisCache, SimpleCache
from config import REDIS_SERVER

redis_cache_provider = RedisCache(**REDIS_SERVER)
memory_cache_provider = SimpleCache(1)


def ssl_required(f):
    @functools.wraps(f)
    def decorator(*args, **kwargs):
        parsed_url = urlparse.urlparse(request.url)
        if parsed_url.scheme != "https" and request.remote_addr != "127.0.0.1":
            secure_url = request.url.replace("http://", "https://")
            return redirect(secure_url)
        return f(*args, **kwargs)

    return decorator


def admin_required(f):
    @functools.wraps(f)
    def decorator(*args, **kwargs):
        parsed_url = urlparse.urlparse(request.url)
        if parsed_url.scheme != "https" and request.remote_addr != "127.0.0.1":
            secure_url = request.url.replace("http://", "https://")
            return redirect(secure_url)
        return f(*args, **kwargs)

    return decorator


def check_etag(f):
    @functools.wraps(f)
    def decorator(*args, **kwargs):
        etag = request.headers.get("If-None-Match")
        if etag == kwargs[kwargs.keys()[0]]:
            return Response(status=304)
        return f(*args, **kwargs)

    return decorator


########################################################################################################################
CASH_TTL = 1000
########################################################################################################################

import functools
from werkzeug.contrib.cache import RedisCache, SimpleCache

_memory_cache_provider = SimpleCache()

'''
Usage:
@memory_cached       #ttl is 300 sec
@memory_cached(ttl)  #ttl is an integer representing Time To Live in ms
'''


def redis_cached(ttl):
    def redis_cached(f):
        @functools.wraps(f)
        def decorator(*args, **kwargs):
            key = "{0}{1}".format(f.__name__, str(args[0:]))
            result = redis_cache_provider.get(key)
            if result is not None:
                return result
            result = f(*args, **kwargs)
            redis_cache_provider.add(key, result, ttl)
            return result

        return decorator

    if callable(ttl):
        f = ttl
        ttl = CASH_TTL
        return redis_cached(f)
    return redis_cached

def memory_cached(ttl):
    def memory_cached(f):
        @functools.wraps(f)
        def decorator(*args, **kwargs):
            key = "{0}{1}".format(f.__name__, str(args[1:]))
            result = _memory_cache_provider.get(key)
            if result is not None:
                return result
            result = f(*args, **kwargs)
            _memory_cache_provider.add(key, result, ttl)
            return result

        return decorator

    if callable(ttl):
        f = ttl
        ttl = CASH_TTL
        return memory_cached(f)
    return memory_cached


def singleton(class_):
    instances = {}

    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]

    return getinstance