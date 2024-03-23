import redis

class RedisQueueItemCompleteError(Exception):
    def __init__(self, queue, queue_key, item):
        self._queue = queue
        self._queue_key = queue_key
        self._item = item
    
    def __str__(self):
        return "{2}: Item {1} in {0} was not completed cleanly".format(
                self._queue_key, self._item,
                type(self).__name__)

class SortaResilientRedisQueue:
    def __init__(self, queue_key, discriminator=None, *, redis_client=None):
        if discriminator is None:
            discriminator = "processing"
        
        self._key = queue_key
        self._backup_key = queue_key + ":" + discriminator
        
        self._redis = redis_client
    
    def push(self, v):
        self._redis.lpush(self._key, p)
    
    def take(self):
        v = self._redis.rpoplpush(self._key, self._backup_key)
        return self._decode(v)
    
    def complete(self, v):
        res = self._redis.lrem(self._backup_key, 1, v)
        if res != 1:
            raise RedisQueueItemCompleteError(
                self, self._key, v)
    
    def _decode(self, v):
        if isinstance(v, bytes):
            v = self._redis.get_encoder().decode(v, force=True)
        return v
