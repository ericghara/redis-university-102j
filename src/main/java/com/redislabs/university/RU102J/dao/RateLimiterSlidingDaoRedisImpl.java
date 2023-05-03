package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.time.Instant;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {

        Instant now = Instant.now();

        long expired = now.toEpochMilli() - windowSizeMS;
        String key = RedisSchema.getSlidingLimiterKey( name, maxHits );

        // START CHALLENGE #7
        Response<Long> size;
        try (Jedis jedis = jedisPool.getResource() ) {
            Transaction trans = jedis.multi();

            trans.zremrangeByScore(name, Double.MIN_VALUE, expired);
            trans.zadd( key, now.toEpochMilli(), now.toString() );
            trans.pexpire(key, windowSizeMS);
            size = trans.zcard( key );

            trans.exec();
        }
        if (size.get() > maxHits) {
            throw new RateLimitExceededException();
        }
    }
}
