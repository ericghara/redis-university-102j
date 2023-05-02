package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import java.util.*;
import java.util.stream.Collectors;

public class SiteDaoRedisImpl implements SiteDao {
    private final JedisPool jedisPool;

    public SiteDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // When we insert a site, we set all of its values into a single hash.
    // We then store the site's id in a set for easy access.
    @Override
    public void insert(Site site) {
        try (Jedis jedis = jedisPool.getResource()) {
            String hashKey = RedisSchema.getSiteHashKey( site.getId() );
            String siteIdKey = RedisSchema.getSiteIDsKey();
            jedis.hmset( hashKey, site.toMap() );
            jedis.sadd( siteIdKey, hashKey );
        }
    }

    @Override
    public Site findById(long id) {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = RedisSchema.getSiteHashKey( id );
            Map<String, String> fields = jedis.hgetAll( key );
            if (fields == null || fields.isEmpty()) {
                return null;
            } else {
                return new Site( fields );
            }
        }
    }

    // Challenge #1
    @Override
    @SuppressWarnings( "unchecked" )
    public Set<Site> findAll() {
        List<Object> rawSites;
        try (Jedis jedis = jedisPool.getResource()) {
            String siteIdKey = RedisSchema.getSiteIDsKey();
            Set<String> siteKeys = jedis.smembers( siteIdKey );
            Pipeline pipeline = jedis.pipelined();
            for (String key : siteKeys) {
                pipeline.hgetAll( key );
            }
            rawSites = pipeline.syncAndReturnAll();
        }
        return rawSites.stream().map(o -> (Map<String,String>) o)
                .filter(site -> Objects.nonNull(site) && !site.isEmpty() )
                .map( Site::new )
                .collect( Collectors.toSet());
    }
}
