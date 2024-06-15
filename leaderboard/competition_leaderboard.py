import redis

class CompetitionLeaderboard:
    def __init__(self, key, *, redis_client):
        self._key = key
        self._redis = redis_client
    
    def get_placement(self, member):
        zscore = self._redis.zscore(self._key, member)
        
        zrrange = self._redis.zrevrangebyscore(self._key, zscore, zscore, start=0, num=1)
        
        crank_member = self._decode(zrrange[0])
        
        zrank_competition = self._redis.zrevrank(crank_member)
        zrank_individual = self._redis.zrevrank(member)
        
        return zrank_individual, zrank_competition + 1, zscore
    
    def get_rank(self, member):
        rank, _ = self._get_rank_and_score(member)
        return rank
    
    def get_score(self, member):
        return self._redis.zscore(self._key, member)
    
    def set_score(self, member, score):
        return self._redis.zadd(self._key, {member: score})
    
    def get_standings(self):
        zmembers_with_scores = self._redis.zrevrange(self._key, 0, -1, withscores=True)
        
        standings = []
        current_score, current_rank, current_run = None, 1, 0
        
        for zmember in zmembers_with_scores:
            member, score = self._decode(zmember[0]), zmember[1]
            if score == current_score:
                current_run += 1
            else:
                current_score = score
                current_rank, current_run = current_rank + current_run, 1
            
            standings.append((member, current_rank, score))
        
        return standings
    
    def _get_rank_and_score(self, member):
        zscore = self._redis.zscore(self._key, member)
        
        if zscore is None:
            return None, None
        
        zrrange = self._redis.zrevrangebyscore(self._key, zscore, zscore, start=0, num=1)
        zrrank = self._redis.zrevrank(self._key, zrrange[0])
        
        return zrrank + 1, zscore
    
    def _decode(self, value):
        if isinstance(value, bytes):
            value = self._redis.get_encoder().decode(value, force=True)
        return value

class LuaCompetitionLeaderboard:
    def __init__(self, key, *, redis_client=None):
        self._key = key
        self._redis = redis_client
        
        self._lua_get_rank_and_score = self._redis.register_script("""
        local zscore = redis.call('ZSCORE', KEYS[1], ARGV[1])
        if not zscore then
            return nil
        end
        local zscoremember = redis.call('ZREVRANGEBYSCORE', KEYS[1], zscore, zscore, 'LIMIT', 0, 1)
        local zscoretoprank = redis.call('ZREVRANK', KEYS[1], zscoremember[1])
        return {zscore, zscoretoprank}
        """)
        self._lua_get_score = self._redis.register_script("""
        return redis.call('ZSCORE', KEYS[1], ARGV[1])
        """)
        self._lua_set_score = self._redis.register_script("""
        return redis.call('ZADD', KEYS[1], ARGV[1])
        """)
        self._lua_get_standings = self._redis.register_script("""
        local standings = {}
        local zmembers = redis.call('ZREVRANGE', KEYS[1], 0, -1)
        local rank = 1
        local run = 0
        local score = nil
        for index, zmember in ipairs(zmembers) do
            local zscore = redis.call('ZSCORE', KEYS[1], zmember)
            if zscore ~= score then
                rank = index
            end
            local standing = {zmember, rank, zscore}
            table.insert(standings, standing)
        end
        return standings
        """)
    
    def get_rank(self, member):
        rank_score = self._get_rank_and_score(member)
        if rank_score is None:
            return None
        return rank_score[0]
    
    def get_score(self, member):
        zscore = self._lua_get_score(keys=[self._key], args=[member])
        return None if zscore is None else float(self._decode(zscore))
    
    def set_score(self, member, score):
        self._lua_set_score(keys=[self._key], args=[member, score])
    
    def _get_rank_and_score(self, member):
        response = self._lua_get_rank_and_score(keys=[self._key], args=[member])
        if response is None:
            return None
        zrank, zscore = int(self._decode(response[0])), response[1]
        return zrank + 1, zscore
    
    def get_standings(self):
        standings = []
        response = self._lua_get_standings(keys=[self._key])
        
        for row in response:
            standings.append([
                self._decode(row[0]),
                int(self._decode(row[1])),
                row[2]
            ])
        
        return standings
    
    def _decode(self, value):
        if isinstance(value, bytes):
            value = self._redis.get_encoder().decode(value, force=True)
        return value
