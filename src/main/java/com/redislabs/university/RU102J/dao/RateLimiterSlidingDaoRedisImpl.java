package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.core.KeyHelper;
import redis.clients.jedis.*;

import java.util.UUID;

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
        // START CHALLENGE #7
        try (Jedis jedis = jedisPool.getResource()) {
            String keyName = "limiter" + ":" + windowSizeMS + ":" + name + ":" + maxHits;
            String key = KeyHelper.getKey(keyName);

            final Transaction transaction = jedis.multi();
            final long currentTimeStamp = System.currentTimeMillis();
            final UUID uuid = UUID.randomUUID();
            final String member = "" + currentTimeStamp + "-" + uuid;
            transaction.zadd(key, currentTimeStamp, member);
            transaction.zremrangeByScore(key,0,  currentTimeStamp - windowSizeMS);
            final Response<Long> hits = transaction.zcard(key);
            transaction.exec();

            if (hits.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }
}
