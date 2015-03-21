package org.jerry.redis.general;

import org.apache.commons.lang.StringUtils;
import org.jerry.redis.util.ICacheDefine;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import com.alibaba.fastjson.JSON;


/**
 * Created by Administrator on 14-11-18.
 */
public class JedisConn implements ICacheDefine{

    private JedisPool pool;


    public void set(String key,Object obj,int time){
        Jedis jedis = null;
        try{
            jedis = getConn();
            jedis.set(key, JSON.toJSONString(obj));
            jedis.expire(key, time);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            closeConn(jedis);
        }
    }

    /****
     * Remove key expire time,make it persist
     * @param key
     */
    public void persist(String key){
    	if(StringUtils.isEmpty(key)){
    		return ;
    	}
    	Jedis jedis = null;
    	try{
    		jedis = getConn();
    		jedis.persist(key);
    	}catch(Exception ex){
    		ex.printStackTrace();
    	}finally{
    		closeConn(jedis);
    	}
    }

    /****
     * Return key's expire time
     * @param key
     * @return
     */
    public Long TTL(String key){
    	if(StringUtils.isEmpty(key)){
    		return -1L;
    	}
    	Jedis jedis = null;
    	try{
    		jedis = getConn();
    		jedis.ttl(key);
    	}catch(Exception ex){
    		ex.printStackTrace();
    	}finally{
    		closeConn(jedis);
    	}
    	return -1L;
    }
    
    public void renameKey(String key,String newKey){
    	if(StringUtils.isEmpty(key) || StringUtils.isEmpty(newKey)){ 
    		return ;
    	}
    	Jedis jedis = null;
    	try{
    		jedis = getConn();
    		jedis.rename(key, newKey);
    	}catch(Exception ex){
    		ex.printStackTrace();
    	}finally{
    		closeConn(jedis);
    	}
    }

    
    /***
     * new key should not be exist
     * @param key
     * @param newKey
     */
    public void renameKeyNX(String key,String newKey){
    	if(StringUtils.isEmpty(key) || StringUtils.isEmpty(newKey)){ 
    		return ;
    	}
    	Jedis jedis = null;
    	try{
    		jedis = getConn();
    		if(!jedis.exists(newKey)){
    			/****
    			 * NewKey must not be exists
    			 */
    			jedis.renamenx(key, newKey);
    		}
    	}catch(Exception ex){
    		ex.printStackTrace();
    	}finally{
    		closeConn(jedis);
    	}
    }
    
    public void expire(String key,int time){
        Jedis jedis = null;
        try{
            jedis = getConn();
            jedis.expire(key, time);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            closeConn(jedis);
        }
    }

    public void del(String key){
        Jedis jedis = null;
        try{
            jedis = getConn();
            jedis.del(key);
        } catch (Exception e){
            closeConn(jedis);
        }

    }

    public boolean has(String key){
        Jedis jedis = null;
        try{
            jedis = getConn();
            return jedis.exists(key);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            closeConn(jedis);
        }
        return false;
    }

    public boolean has(String key,int time){
        Jedis jedis = null;
        try{
            jedis = getConn();
            if( jedis.exists(key)){
                jedis.expire(key,time) ;
                return true;
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {

        }
        return false;
    }

    public void closeConn(Jedis jedis){
        if(jedis!=null){
            pool.returnResource(jedis);
            /***
             * Ask the server to close the connection. 
             * The connection is closed as soon as all pending replies have been written to the client.
             */
            //jedis.quit();
            jedis.disconnect();
            jedis = null;
        }
    }
    public Jedis getConn(){
        if(pool==null ){
            // 建立连接池配置参数
            JedisPoolConfig config = new JedisPoolConfig();

            // 设置最大连接数
            config.setMaxActive(200);

            // 设置最大阻塞时间，记住是毫秒数milliseconds
            config.setMaxWait(1000);

            // 设置空间连接
            config.setMaxIdle(10);

            // 创建连接池
            pool = new JedisPool(config,DEFAULT_HOST_LINUX);

        }

        Jedis jedis = pool.getResource();
        jedis.connect();

        return jedis;
    }

}
