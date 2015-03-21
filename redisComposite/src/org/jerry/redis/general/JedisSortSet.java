package org.jerry.redis.general;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;

import com.alibaba.fastjson.JSON;

import redis.clients.jedis.Jedis;

public class JedisSortSet<T extends Object> extends JedisConn {

	private Class<T> clazz;
	
	public JedisSortSet(Class<T> clazz){
		this.clazz = clazz;
	}
	
	
	public void addMembers(String key ,Map<Double,T> map){
		addMembers(key, map, 0);
	}
	
	/****
	 * Adds all the specified members with the specified scores to the sorted set stored at key. 
	 * It is possible to specify multiple score / member pairs. If a specified member is already 
	 * a member of the sorted set, the score is updated and 
	 * the element reinserted at the right position to ensure the correct ordering.
     *If key does not exist, a new sorted set with the specified members as sole members is created, like if the sorted set was empty. 
     *If the key exists but does not hold a sorted set, an error is returned.
     *The score values should be the string representation of a double precision 
     *floating point number. +inf and -inf values are valid values as well.
	 * @param key
	 * @param map
	 * @param expireSec
	 */
	public void addMembers(String key ,Map<Double,T> map,int expireSec){
		if(StringUtils.isEmpty(key) || map == null || map.isEmpty()){
			return ;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			Map<Double,String> mapStr = new HashMap<Double, String>();
			for(Entry<Double, T> entry : map.entrySet()){
				mapStr.put(entry.getKey(), JSON.toJSONString(entry.getValue()));
			}
			jedis.zadd(key, mapStr);
			if(expireSec > 0){
				jedis.expire(key, expireSec);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
	}

	
	public void addMember(String key,Double score,T t){
		addMember(key, score, t, 0);
	}
	
	/****
	 * Add single member to stored set
	 * @param key
	 * @param score
	 * @param t
	 */
	public void addMember(String key,Double score ,T t,int expireSec){
		Jedis jedis = null;
		try{
			jedis = getConn();
			jedis.zadd(key,score,JSON.toJSONString(t));
			if(expireSec > 0){
				jedis.expire(key, expireSec);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
	}
	
	
	
	/***
	 * Returns the sorted set cardinality (number of elements) of the sorted set stored at key
	 * @param key
	 * @return
	 */
	public Long membersCount(String key){
		if(StringUtils.isEmpty(key)){
			return 0L;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			return jedis.zcard(key);
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
		return 0L;
	}
	
}

