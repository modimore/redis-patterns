import redis

class RedisLockNotHeldError(Exception):
    def __init__(self, lock, key, value):
        self._lock = lock
        self._key = key
        self._value = value
    
    def __str__(self):
        return "{2}: Lock '{0}' not held (identifier: '{1}', type: {3})".format(
                self._key, self._value, type(self).__name__,
                type(self._lock).__name__)

class RedisLockReleaseError(Exception):
    def __init__(self, lock, key, value):
        self._lock = lock
        self._key = key
        self._value = value
    
    def __str__(self):
        return "{2}: Lock was '{0}' not released cleanly (identifier: '{1}', type: {3})".format(
                self._key, self._value, type(self).__name__,
                type(self._lock).__name__)

class RedisLockRefreshError(Exception):
    def __init__(self, lock, key, value):
        self._lock = lock
        self._key = key
        self._value = value
    
    def __str__(self):
        return "{2}: Lock was '{0}' not refreshed (identifier: '{1}', type: {3})".format(
                self._key, self._value, type(self).__name__,
                type(self._lock).__name__)

class RedisLock:
    def __init__(self, key, value, *, duration=1, redis_client=None):
        self._key = key
        self._value = value
        
        self._duration = duration
        self._redis = redis_client

    def check(self):
        v = self._redis.get(self._key)
        if isinstance(v, bytes):
            v = self._redis.get_encoder().decode(v, force=True)
        return v == self._value
    
    def acquire(self):
        return self._redis.set(self._key, self._value,
                nx=True, ex=self._duration)
    
    def release(self):
        pipeline = self._redis.pipeline(transaction=False)
        pipeline.watch(self._key)
        
        # If the lock is not held by this lock instance,
        # let the caller know explicitly with an exception
        if not self.check():
            pipeline.unwatch()
            raise RedisLockNotHeldError(self, self._key, self._value)
        
        pipeline.multi()
        pipeline.delete(self._key)
        
        try:
            res = pipeline.execute()
        except redis.exceptions.WatchError:
            raise RedisLockReleaseError(self, self._key, self._value)
        
        released = bool(res[0])
        if not released:
            raise RedisLockReleaseError(self, self._key, self._value)

class RefreshableRedisLock(RedisLock):
    def refresh(self):
        pipeline = self._redis.pipeline(transaction=False)
        pipeline.watch(self._key)
        
        if not self.check():
            pipeline.unwatch()
            raise RedisLockNotHeldError(self, self._key, self._value)
        
        pipeline.multi()
        pipeline.expire(self._key, self._duration)
        
        try:
            res = pipeline.execute()
        except redis.exceptions.WatchError:
            raise RedisLockRefreshError(self, self._key, self._value)
        
        refreshed = bool(res[0])
        if not refreshed:
            raise RedisLockRefreshError(self, self._key, self._value)

class RedisLuaLock:
    def __init__(self, key, value, *, duration, redis_client=None):
        self._key = key
        self._value = value
        
        self._duration = duration
        self._redis = redis_client
        
        self._lua_check = self._redis.register_script("""
        local holder = redis.call('GET', KEYS[1])
        return holder == ARGV[1]
        """)
        self._lua_acquire = self._redis.register_script("""
        return redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2])
        """)
        self._lua_release = self._redis.register_script("""
        local v = redis.call('GET', KEYS[1])
        if not v or v ~= ARGV[1] then
            return 0
        end
        redis.call('DEL', KEYS[1])
        return 1
        """)
    
    def check(self):
        return bool(self._lua_check(keys=[self._key], args=[self._value, self._duration]))
    
    def acquire(self):
        return bool(self._lua_acquire(keys=[self._key], args=[self._value, self._duration]))
    
    def release(self):
        res = self._lua_release(keys=[self._key], args=[self._value])
        if not bool(res):
            raise RedisLockNotHeldError(self, self._key, self._value)

class RefreshableRedisLuaLock(RedisLuaLock):
    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        
        self._lua_refresh = self._redis.register_script("""
        local holder = redis.call('GET', KEYS[1])
        if holder ~= ARGV[1] then
            return 0
        end
        redis.call('EXPIRE', KEYS[1], ARGV[2])
        return 1
        """)
    
    def refresh(self):
        refreshed = bool(self._lua_refresh(keys=[self._key], args=[self._value, self._duration]))
        if not refreshed:
            raise RedisLockNotHeldError(self, self._key, self._value)
