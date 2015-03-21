package org.jerry.redis.shared;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jerry.redis.shared.pool.SharedPool;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

import com.alibaba.fastjson.JSON;

public class ShardedJedisMap<T extends Object>  extends SharedPool{

	  private Class<T> clazz;


	    public ShardedJedisMap(Class<T> clazz){
	        this.clazz = clazz;
	    }
	    
	    /****
	     * GET all the Map value 
	     * @param key
	     * @return
	     */
	    public Map<String,T> getMap(String key){
	        ShardedJedis jedis = null;
	        try{
	            jedis = getConn();
	            Map<String,String> map = jedis.hgetAll(key);
	            Map<String,T> tMap = new HashMap<String, T>();
	            for(Map.Entry<String ,String> entry : map.entrySet()) {
	                T t = JSON.parseObject(entry.getValue(),clazz);
	                tMap.put(entry.getKey(),t);
	            }
	            return tMap;
	        } catch (Exception e){
	            e.printStackTrace();
	        } finally {
	            closeConn(jedis);
	        }
	        return null;
	    }


	    /***
	     * return the map's field value
	     * @param key
	     * @param field
	     * @return
	     */
	    public T getMapValue(String key,String field){
	    	if(StringUtils.isEmpty(key) || StringUtils.isEmpty(field)){
	    		return null;
	    	}
	    	ShardedJedis jedis = null;
	        try{
	            jedis = getConn();
	            String val = jedis.hget(key,field);
	            if(!StringUtils.isEmpty(val))  return JSON.parseObject(val,clazz) ;
	        } catch (Exception e){
	            e.printStackTrace();
	        } finally {
	            closeConn(jedis);
	        }
	        return null;

	    }

	    public void setMap(String key,Map<String ,T> map){
	    	setMap(key, map, null);
	    }
	    
	    
	    /****
	     * Store the map to redis map
	     * @param key
	     * @param map
	     * @param expireSec
	     */
	    public void setMap(String key,Map<String ,T> map,Integer expireSec){
	    	if(StringUtils.isEmpty(key) || map == null ||  map.isEmpty()){
	    		return ;
	    	}
	    	ShardedJedis jedis = null;
	        try{
	            jedis = getConn();
	            for(Map.Entry<String ,T> entry : map.entrySet()) {
	                jedis.hset(key,entry.getKey(),JSON.toJSONString(entry.getValue()));
	            }
	            if(expireSec != null && expireSec > 0){
	                jedis.expire(key, expireSec);
	            }
	        
	        } catch (Exception e){
	            e.printStackTrace();
	        } finally {
	            closeConn(jedis);
	        }
	    }


	    /*****
	     * pipeline use for large map set
	     * @param key
	     * @param map
	     */
	    public void setMapPipeline(String key,Map<String,T> map ){
	    	setMapPipeline(key, map, null);
	    }

	    /*****
	     * pipeline use for large map set
	     * @param key
	     * @param map
	     * @param time expire seconds
	     */
	    public void setMapPipeline(String key,Map<String,T> map , Integer expireSec){
	    	if(StringUtils.isEmpty(key) || map == null || map.isEmpty()){
	    		return ;
	    	}
	    	ShardedJedis jedis = null;
	    	ShardedJedisPipeline pipeline = null;
	        try{
	            jedis = getConn();
	            pipeline = new ShardedJedisPipeline();
	            pipeline.setShardedJedis(jedis);
	            for(Map.Entry<String , T> entry : map.entrySet()){
	                pipeline.hset(key,entry.getKey(),JSON.toJSONString(entry.getValue()));
	            }
	            if(expireSec != null && expireSec > 0){
	            	  pipeline.expire(key,expireSec);
	            }
	            pipeline.sync();
	        }catch (Exception ex){
	            ex.printStackTrace();
	        }finally {
	            closeConn(jedis);
	            pipeline = null;
	        }
	    }

	    
	    
	    public void setMapValue(String key,String field,T t){
	        setMapValue(key, field, t, null);

	    }
	    
	    /****
	     * Set Map field value
	     * @param key
	     * @param field
	     * @param t
	     * @param expireSec
	     */
	    public void setMapValue(String key,String field,T t,Integer expireSec){
	    	if(StringUtils.isEmpty(key) || StringUtils.isEmpty(field) || t == null ){
	    		return ;
	    	}
	    	ShardedJedis jedis = null;
	        try{
	            jedis = getConn();
	            jedis.hset(key,field,JSON.toJSONString(t));
	            if(expireSec != null && expireSec > 0){
	            	jedis.expire(key, expireSec);
	            }
	        } catch (Exception e){
	            e.printStackTrace();
	        } finally {
	            closeConn(jedis);
	        }
	    }

}
