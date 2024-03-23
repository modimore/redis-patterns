import redis

class RedisQueueItemNotClaimedError(Exception):
    def __init__(self, queue, queue_key, consumer_name, item):
        self._queue = queue
        self._queue_key = queue_key
        self._consumer_name = consumer_name
        self._item = item
    
    def __str__(self):
        return "{3}: Item {2} in {0} not claimed by consumer {1}".format(
                self._queue_key, self._consumer_name, self._item,
                type(self).__name__)

class RedisQueueItemCompleteError(Exception):
    def __init__(self, queue, queue_key, consumer_name, item):
        self._queue = queue
        self._queue_key = queue_key
        self._consumer_name = consumer_name
        self._item = item
    
    def __str__(self):
        return "{3}: Item {2} in {0} was not completed cleanly".format(
                self._queue_key, self._consumer_name, self._item,
                type(self).__name__)

class CircularLockingRedisQueue:
    def __init__(self, queue_key, consumer_name, *, duration=None, redis_client=None):
        self._key = queue_key
        self._discriminator = consumer_name
        
        self._duration = duration
        
        self._redis = redis_client
    
    def push(self, v):
        self._redis.lpush(self._key, v)
    
    def take(self):
        v = self._redis.rpoplpush(self._key, self._key)
        v = self._decode(v)
        
        if v is None:
            return None
        
        locked = self._redis.set(self._item_lock_key(v), self._discriminator,
                nx=True, ex=self._duration)
        
        return v if locked else None
    
    def complete(self, v):
        lock_key = self._item_lock_key(v)
        
        pipeline = self._redis.pipeline(transaction=False)
        pipeline.watch(self._key, lock_key)
        
        lock_value = self._redis.get(lock_key)
        lock_value = self._decode(lock_value)
        
        if lock_value != self._discriminator:
            pipeline.unwatch()
            raise RedisQueueItemNotClaimedError(
                self, self._key, self._discriminator, v)
        
        pipeline.multi()
        
        pipeline.lrem(self._key, 1, v)
        pipeline.delete(lock_key)
        
        try:
            res = pipeline.execute()
        except redis.exceptions.WatchError:
            raise RedisQueueItemCompleteError(
                self, self._key, self._discriminator, v)
        
        removed, released = int(res[0]), bool(res[1])
        
        if removed != 1:
            raise RedisQueueItemCompleteError(
                self, self._key, self._discriminator, v)
        
        if not released:
            raise RedisQueueItemCompleteError(
                self, self._key, self._discriminator, v)
    
    def _decode(self, v):
        if isinstance(v, bytes):
            v = self._redis.get_decoder().decode(v, force=True)
        return v
    
    def _item_lock_key(self, v):
        return self._key + ":lock:" + v
