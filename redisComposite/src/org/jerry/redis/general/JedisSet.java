package org.jerry.redis.general;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import com.alibaba.fastjson.JSON;

public class JedisSet<T extends Object> extends JedisConn{

	private Class<T> clazz;
	
	public JedisSet(Class<T> clazz){
		this.clazz = clazz;
	}
	
	public void addMember(String key,T t){
		addMember(key, t , 0);
	}
	
	
	/****
	 * Add member to set
	 * @param key
	 * @param t
	 * @param time
	 */
	public void addMember(String key,T t, int expireSec){
		if(StringUtils.isEmpty(key) || t == null ){
			return ;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			jedis.sadd(key, JSON.toJSONString(t));
			if(expireSec > 0){
				jedis.expire(key, expireSec);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
	}

	
	public void addMember(String key,List<T> list){
		addMember(key, list, 0);
	}
	
	
 	/****
	 * Add member to set , members come from list
	 * @param key
	 * @param t
	 * @param time
	 */
	public void addMember(String key,List<T> list, int expireSec){
		if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
			return ;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
		    for (T t2 : list) {
		    	jedis.sadd(key, JSON.toJSONString(t2));
			}
			if(expireSec > 0){
				jedis.expire(key, expireSec);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
	}

	
	public void addMembeUsePipeline(String key,List<T> list){
		addMemberUsePipeline(key, list, DEFAULT_PORT_LINUX);
	}
	
	/****
	 * Add member to set , members come from list,Use pipeline
	 * @param key
	 * @param t
	 * @param time
	 */
	public void addMemberUsePipeline(String key,List<T> list, int expireSec){
		if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
			return ;
		}
		Jedis jedis = null;
		Pipeline pipeline = null;
		try{
			jedis = getConn();
			pipeline = new Pipeline();
			pipeline.setClient(jedis.getClient());
		    for (T t2 : list) {
		    	pipeline.sadd(key, JSON.toJSONString(t2));
			}
			if(expireSec > 0){
				pipeline.expire(key, expireSec);
			}
			pipeline.sync();
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
			pipeline = null;
		}
	}
	
	public void addMemberUsePipelineAndTransaction(String key,List<T> list){
		addMemberUsePipelineAndTransaction(key, list, 0);
	}
	
	
	/****
	 * Add member to set , members come from list,Use pipeline
	 * @param key
	 * @param t
	 * @param time
	 */
	public void addMemberUsePipelineAndTransaction(String key,List<T> list, int expireSec){
		if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
			return ;
		}
		Jedis jedis = null;
		Pipeline pipeline = null;
		try{
			jedis = getConn();
			pipeline = new Pipeline();
			pipeline.setClient(jedis.getClient());
			pipeline.multi();
		    for (T t2 : list) {
		    	pipeline.sadd(key, JSON.toJSONString(t2));
			}
			if(expireSec > 0){
				pipeline.expire(key, expireSec);
			}
			pipeline.exec();
			pipeline.sync();
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
			pipeline = null;
		}
	}
	
	/****
	 * Get the number of members in a set
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
			return jedis.scard(key);
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
		return 0L;
	}

	/****
	 * Returns the members of the set resulting from the difference between the first set and all the successive sets.
	 *	For example:
	 *	key1 = {a,b,c,d}
	 *	key2 = {c}
	 *	key3 = {a,c,e}
	 *	SDIFF key1 key2 key3 = {b,d}
	 * @param keys
	 * @return
	 */
	public List<T> differencesCompareToSuccessiveSets(String... keys){
		if(keys == null || keys.length == 0){
			return  null;
		}
		Jedis jedis = null;
		List<T> differences = new ArrayList<T>();
		try{
			jedis = getConn();
			Set<String> strSets = jedis.sdiff(keys);
			if(strSets != null && strSets.isEmpty()){
				Iterator<String> ite = strSets.iterator();
				while(ite.hasNext()){
					String json = ite.next();
					if(!StringUtils.isEmpty(json)){
						differences.add(JSON.parseObject(json, clazz));
					}
				}
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
		return differences;
	}

	public void storeDifferencesCompareToSuccessiveSets(String storeKey,String... keys){
		storeDifferencesCompareToSuccessiveSets(storeKey, 0 , keys);
	}
	
	/****
	 * This command is equal to SDIFF, but instead of returning the resulting set, 
	 * it is stored in destination.
     * If destination already exists, it is overwritten.
	 * @param storeKey
	 * @param expireSec
	 * @param keys
	 */
	public void storeDifferencesCompareToSuccessiveSets(String storeKey,int expireSec,String... keys){
		if(keys == null || keys.length == 0 || StringUtils.isEmpty(storeKey)){
			return;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			jedis.sdiffstore(storeKey, keys);
			if(expireSec > 0 ){
				jedis.expire(storeKey, expireSec);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
	}


	/****
	 * Returns the members of the set resulting from the intersection of all the given sets.
     *For example:
     *key1 = {a,b,c,d}
     *key2 = {c}
     *key3 = {a,c,e}
     *SINTER key1 key2 key3 = {c}
	 * @param keys
	 * @return
	 */
	public List<T> intersections(String... keys){
		if(keys == null || keys.length == 0){
			return  null;
		}
		Jedis jedis = null;
		List<T> intersec = new ArrayList<T>();
		try{
			jedis = getConn();
			Set<String> strSets = jedis.sinter(keys);
			if(strSets != null && strSets.isEmpty()){
				Iterator<String> ite = strSets.iterator();
				while(ite.hasNext()){
					String json = ite.next();
					if(!StringUtils.isEmpty(json)){
						intersec.add(JSON.parseObject(json, clazz));
					}
				}
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
		return intersec;
	}


	public void storeIntersections(String storeKey,String... keys){
		storeIntersections(storeKey, 0, keys);
	}
	
	/****
	 * This command is equal to SINTER, but instead of returning the resulting set, it is stored in destination.
	 * If destination already exists, it is overwritten.
	 * @param storeKey
	 * @param expireSec
	 * @param keys
	 */
	public void storeIntersections(String storeKey,int expireSec,String... keys){
			if(keys == null || keys.length == 0 || StringUtils.isEmpty(storeKey)){
				return;
			}
			Jedis jedis = null;
			try{
				jedis = getConn();
				jedis.sinterstore(storeKey, keys);
				if(expireSec > 0 ){
					jedis.expire(storeKey, expireSec);
				}
			}catch(Exception ex){
				ex.printStackTrace();
			}finally{
				closeConn(jedis);
			}
	}

	/****
	 * Returns if member is a member of the set stored at key
	 * @param key
	 * @param t
	 * @return
	 */
	public boolean isMemeber(String key,T t){
		if(StringUtils.isEmpty(key) || t == null ){
			return false;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			return jedis.sismember(key, JSON.toJSONString(t));
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
		return false;
	}


	/****
	 * Returns all the members of the set value stored at key.
	 * @param key
	 * @return
	 */
	public List<T> members(String key){
		if(StringUtils.isEmpty(key)){
			return null;
		}
		Jedis jedis = null;
		List<T> members = new ArrayList<T>();
		try{
			jedis = getConn();
			Set<String> strSets = jedis.smembers(key);
			if(strSets != null && strSets.isEmpty()){
				Iterator<String> ite = strSets.iterator();
				while(ite.hasNext()){
					String json = ite.next();
					if(!StringUtils.isEmpty(json)){
						members.add(JSON.parseObject(json, clazz));
					}
				}
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
		return members;
	}

	
	
	
	/****
	 * Move member from source set to destination set
	 * @param sourceKey
	 * @param destKey
	 * @param t
	 */
	public void move(String sourceKey,String destKey,T t){
		if(StringUtils.isEmpty(sourceKey) || StringUtils.isEmpty(destKey) || t == null ){
			return ;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			jedis.smove(sourceKey, destKey, JSON.toJSONString(t));
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
	}

	
	/****
	 * Return random member of set ,also remove it
	 * @param key
	 * @return
	 */
	public T pop(String key){
		if(StringUtils.isEmpty(key)){
			return null;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			String json = jedis.spop(key);
			if(!StringUtils.isEmpty(json)){
				return JSON.parseObject(json, clazz);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
		return null;
	}
	
	/****
	 * Return random given count member from the key's set
	 * @param key
	 * @param count
	 * @return
	 */
	public T randMember(String key){
		if(StringUtils.isEmpty(key)){
			return null;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			String json = jedis.srandmember(key);
			if(!StringUtils.isEmpty(json)){
				return JSON.parseObject(json, clazz);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
		return null;
	}


	/****
	 * Remove members from the set
	 * @param key
	 * @param list
	 */
	public void remove(String key,List<T> list){
		if(StringUtils.isEmpty(key)){
			return ;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			for (T t : list) {
				jedis.srem(key, JSON.toJSONString(t));
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
	}
	
	
	/***
	 * Union given set
	 * @param keys
	 * @return
	 */
	public List<T> union(String... keys){
		if(keys == null || keys.length == 0){
			return null;
		}
		Jedis jedis = null;
		List<T> members = new ArrayList<T>();
		try{
			jedis = getConn();
			Set<String> strSets = jedis.sunion(keys);
			if(strSets != null && strSets.isEmpty()){
				Iterator<String> ite = strSets.iterator();
				while(ite.hasNext()){
					String json = ite.next();
					if(!StringUtils.isEmpty(json)){
						members.add(JSON.parseObject(json, clazz));
					}
				}
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
		return members;
	}

	
	public void unionStore(String destKey,String... keys){
		unionStore(destKey, 0, keys);
	}
	
	/****
	 * Union given set ,store it to destination set
	 * @param destKey
	 * @param keys
	 */
	public void unionStore(String destKey,int expireSec,String... keys){
		if(StringUtils.isEmpty(destKey) || keys == null || keys.length == 0){
			return ;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			jedis.sunionstore(destKey, keys);
			if(expireSec > 0 ){
				jedis.expire(destKey, expireSec);
			}
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
	}
}
