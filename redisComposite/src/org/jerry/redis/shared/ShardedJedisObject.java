package org.jerry.redis.shared;

import org.apache.commons.lang.StringUtils;
import org.jerry.redis.shared.pool.SharedPool;

import com.alibaba.fastjson.JSON;

import redis.clients.jedis.ShardedJedis;

public class ShardedJedisObject<T extends Object> extends SharedPool {

	private Class<T> clazz;
	
	public ShardedJedisObject(Class<T> clazz){
		this.clazz = clazz;
	}
	 
	
	/****
	 * return the key's value
	 * @param key
	 * @return
	 */
	public T get(String key){
		if(StringUtils.isEmpty(key)){
			return null;
		}
		ShardedJedis jedis = null;
		try{
			jedis = getConn();
			String object = jedis.get(key);
			return StringUtils.isEmpty(object) ? null : JSON.parseObject(object, clazz);
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
		return null;
	}
	
	/****
	 * Set Jedis key value
	 * @param key
	 * @param value 
	 * @param expireTime
	 */
	public void set(String key,String value,Integer expireTime){
		if(StringUtils.isEmpty(key) || StringUtils.isEmpty(value)){
			return ;
		}
		ShardedJedis jedis = null;
		try{
			jedis = getConn();
			jedis.set(key, value);
			if(expireTime != null && expireTime > 0){
				jedis.expire(key, expireTime);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
	}
	
	/****
	 * Set key value , object convert to string
	 * @param key
	 * @param t
	 * @param expireTime
	 */
	public void set(String key, T t,Integer expireTime){
		if(t == null ){
			return ;
		}
		set(key, JSON.toJSONString(t), expireTime);
		
	}
}
