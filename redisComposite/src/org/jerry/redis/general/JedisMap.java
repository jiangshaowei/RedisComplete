package org.jerry.redis.general;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import com.alibaba.fastjson.JSON;

/****
 * Jedis Map
 * @param <T>
 */
public class JedisMap<T extends Object> extends JedisConn {

    private Class<T> clazz;


    public JedisMap(Class<T> clazz){
        this.clazz = clazz;
    }

    public Map<String,T> getMap(String key){
        Jedis jedis = null;
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


    public T getMapValue(String key,String field){
    	if(StringUtils.isEmpty(key) || StringUtils.isEmpty(field)){
    		return null;
    	}
        Jedis jedis = null;
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
    
    public void setMap(String key,Map<String ,T> map,Integer expireSec){
    	if(StringUtils.isEmpty(key) || map == null ||  map.isEmpty()){
    		return ;
    	}
        Jedis jedis = null;
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


    
    /****
     * Set map ,Use Transaction
     * @param key
     * @param map
     * @param expireSec
     */
    public void setMapUseTransaction(String key,Map<String ,T> map,Integer expireSec){
    	if(StringUtils.isEmpty(key) || map == null ||  map.isEmpty()){
    		return ;
    	}
        Jedis jedis = null;
        Transaction transaction = null;
        try{
            jedis = getConn();
            transaction = jedis.multi();
            for(Map.Entry<String ,T> entry : map.entrySet()) {
            	transaction.hset(key,entry.getKey(),JSON.toJSONString(entry.getValue()));
            }
            if(expireSec != null && expireSec > 0){
            	transaction.expire(key, expireSec);
            }
            transaction.exec();
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
    public void setMapUsePipeline(String key,Map<String,T> map ){
    	setMapUsePipeline(key, map, null);
    }

    /*****
     * pipeline use for large map set
     * @param key
     * @param map
     * @param time expire seconds
     */
    public void setMapUsePipeline(String key,Map<String,T> map , Integer expireSec){
    	if(StringUtils.isEmpty(key) || map == null || map.isEmpty()){
    		return ;
    	}
        Jedis jedis = null;
        Pipeline pipeline = null;
        try{
            jedis = getConn();
            pipeline = new Pipeline();
            pipeline.setClient(jedis.getClient());
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

    public void setMapUsePipelineAndTransaction(String key,Map<String,T> map ){
    	setMapUsePipelineAndTransaction(key, map, null);
    }
    
    /*****
     * pipeline use for large map set,Use transaction
     * @param key
     * @param map
     * @param time expire seconds
     */
    public void setMapUsePipelineAndTransaction(String key,Map<String,T> map , Integer expireSec){
    	if(StringUtils.isEmpty(key) || map == null || map.isEmpty()){
    		return ;
    	}
        Jedis jedis = null;
        Pipeline pipeline = null;
        try{
            jedis = getConn();
            pipeline = new Pipeline();
            pipeline.setClient(jedis.getClient());
            pipeline.multi();
            for(Map.Entry<String , T> entry : map.entrySet()){
                pipeline.hset(key,entry.getKey(),JSON.toJSONString(entry.getValue()));
            }
            if(expireSec != null && expireSec > 0){
            	  pipeline.expire(key,expireSec);
            }
            pipeline.exec();
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
    
    public void setMapValue(String key,String field,T t,Integer expireSec){
    	if(StringUtils.isEmpty(key) || StringUtils.isEmpty(field) || t == null ){
    		return ;
    	}
        Jedis jedis = null;
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
    
    
    public void setMapUseTransaction(String key,Map<String ,T> map){
    	setMapUseTransaction(key, map, null);
    }

}
