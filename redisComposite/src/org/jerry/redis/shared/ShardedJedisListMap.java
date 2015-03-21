package org.jerry.redis.shared;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jerry.redis.shared.pool.SharedPool;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

import com.alibaba.fastjson.JSONArray;

public class ShardedJedisListMap<T extends Object> extends SharedPool{
	
	
	private Class<T> clazz;
	public ShardedJedisListMap(Class<T> clazz) {
		this.clazz = clazz;
	}

	
	/****
	 * Return the map value 
	 * @param key
	 * @return
	 */
	public Map<String, List<T>> getMap(String key) {
        if(StringUtils.isEmpty(key)){
            return null;
        }
        ShardedJedis jedis = null;
		try {
			jedis = getConn();
			Map<String, String> map = jedis.hgetAll(key);
			if (null == map || map.isEmpty())
				return null;
			Map<String, List<T>> tMap = new HashMap<String, List<T>>();
			for (Map.Entry<String, String> entry : map.entrySet()) {
				List<T> list = JSONArray.parseArray(entry.getValue(), clazz);
				tMap.put(entry.getKey(), list);
			}
			return tMap;
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeConn(jedis);
		}
		return null;
	}

	
	/***
	 * Return the single list correspond to given field
	 * @param key
	 * @param field
	 * @return
	 */
	public List<T> getList(String key, String field) {
        if(StringUtils.isEmpty(key) || StringUtils.isEmpty(field)){
            return null;
        }
        ShardedJedis jedis = null;
		try {
			jedis = getConn();
			String val = jedis.hget(key, field);
			if (!StringUtils.isEmpty(val))
				return JSONArray.parseArray(val, clazz);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeConn(jedis);
		}
		return null;

	}

	/*****
	 * add new value into field list
	 * @param key
	 * @param field
	 * @param value
	 */
	public void addValue(String key,String field,T value){
        if(StringUtils.isEmpty(key) || StringUtils.isEmpty(field) || value == null)
            return ;
		List<T> ts = getList(key, field);
		if(ts == null || ts.isEmpty()){
			ts = new ArrayList<T>();
			ts.add(value);
		}else{
			/****
			 * Already existed the list ,just add it to head
			 */
			ts.add(0, value);
		}
		ShardedJedis jedis = null;
		try {
			jedis = getConn();
			jedis.hset(key, field, JSONArray.toJSONString(ts));
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeConn(jedis);
		}
	}

    /****
     * Use the pipe line to set the map value
     * @param key
     * @param map
     * @param time
     */
    public void setMapPipeline(String key,Map<String,List<T>> map ,Integer expireSec){
        if(StringUtils.isEmpty(key) || map == null || map.isEmpty()){
            return ;
        }
        ShardedJedis jedis = null;
        ShardedJedisPipeline pipeline  = null;
        try {
            jedis = getConn();
            pipeline = new ShardedJedisPipeline();
            pipeline.setShardedJedis(jedis);
            for (Map.Entry<String, List<T>> entry : map.entrySet()) {
                if (null == entry || entry.getValue() == null || entry.getKey() == null)
                    continue;
                pipeline.hset(key, entry.getKey(), JSONArray.toJSONString(entry.getValue()));
            }
            if(expireSec != null && expireSec > 0){
                pipeline.expire(key, expireSec);
            }
            pipeline.sync();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConn(jedis);
        }
    }
    
    /****
     * set map value to redis 
     * @param key
     * @param map
     * @param expireSec
     */
	public void setMap(String key, Map<String, List<T>> map, Integer expireSec) {
        if(StringUtils.isEmpty(key)){
            return ;
        }
        ShardedJedis jedis = null;
		try {
			jedis = getConn();
			for (Map.Entry<String, List<T>> entry : map.entrySet()) {
				if (null == entry || entry.getValue() == null || entry.getKey() == null)
					continue;
				jedis.hset(key, entry.getKey(), JSONArray.toJSONString(entry.getValue()));
			}
            if(expireSec != null && expireSec > 0){
                jedis.expire(key, expireSec);
            }
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeConn(jedis);
		}
	}

	public void setList(String key, String field, List<T> list, int time) {
        if(StringUtils.isEmpty(key) || StringUtils.isEmpty(field) || list == null || list.isEmpty()){
            return ;
        }
        ShardedJedis jedis = null;
		try {
			jedis = getConn();
			jedis.hset(key, field, JSONArray.toJSONString(list));
			jedis.expire(key, time);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeConn(jedis);
		}
	}

	
	/****
	 * Delete the key field
	 * @param key
	 * @param field
	 */
	public void del(String key, String... field) {
        if(StringUtils.isEmpty(key) || field == null || field.length == 0){
            return ;
        }
        ShardedJedis jedis = null;
		try {
			jedis = getConn();
			jedis.hdel(key, field);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			closeConn(jedis);
		}
	}
}
