package org.jerry.redis.general;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import com.alibaba.fastjson.JSON;

/****
 * Jedis Object
 * @param <T>
 */
public class JedisObject<T extends Object> extends JedisConn {

    private Class<T> clazz;

    public JedisObject(Class<T> clazz){
        this.clazz = clazz;
    }

    public T get(String key){
        Jedis jedis = null;
        try{
            jedis = getConn();
            String result = jedis.get(key);
            return JSON.parseObject(result,clazz);
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            closeConn(jedis);
        }
        return null;
    }

    /****
     * Generate set
     * @param key
     * @param value
     */
    public void set(String key,String value){
    	if(StringUtils.isEmpty(key) || StringUtils.isEmpty(value)){
    		return ;
    	}
        Jedis jedis = null;
        try{
            jedis = getConn();
            jedis.set(key,value);
        }catch(Exception ex){
            ex.printStackTrace();
        }finally {
            closeConn(jedis);
        }
    }
    
    
    
    /*****
     * set use transaction
     * @param key
     * @param value
     */
    public void setUseTransaction(String key,String value){
    	if(StringUtils.isEmpty(key) || StringUtils.isEmpty(value)){
    		return ;
    	}
        Jedis jedis = null;
        Transaction transaction = null;
        try{
            jedis = getConn();
            jedis.watch(key);
            transaction = jedis.multi();
            transaction.set(key,value);
            transaction.exec();
        }catch(Exception ex){
            ex.printStackTrace();
        }finally {
            closeConn(jedis);
        }
    }
}
