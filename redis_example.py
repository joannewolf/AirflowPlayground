# https://github.com/apache/airflow/tree/master/airflow/providers/redis

import redis
from airflow.contrib.hooks.redis_hook import RedisHook
# from airflow.providers.redis.hooks.redis import RedisHook

# cache = redis.StrictRedis(host='redis', port=6379, db=0)
# print cache.keys()

cache2 = redis = RedisHook().get_conn()
print cache2.keys()