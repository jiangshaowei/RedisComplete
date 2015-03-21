package org.jerry.redis.general;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Transaction;

import com.alibaba.fastjson.JSON;

/****
 * Jedis List,please don't use setList or getList(String , int ,int)
 * unless you need to retrive  sublist of the existing  redis list
 *
 *
 * USE THE JedisListMap
 * @param <T>
 */
public class JedisList<T extends Object> extends JedisConn{

    private Class<T> clazz;


    public JedisList(Class<T> clazz){
        this.clazz = clazz;
    }


    /****
     * return the entire list
     * @param key
     * @return
     */
    public List<T> getList(String key){
        return getList(key,0,-1);
    }

    /****
     * return jedis list begin start to end
     * @param key
     * @param start
     * @param end
     * @return
     */
    public List<T> getList(String key,int start, int end){
        if(StringUtils.isEmpty(key)){
            return null;
        }
        Jedis jedis = null;
        try{
            jedis = getConn();
            List<String> list = jedis.lrange(key,start,end);
            List<T> tList = new ArrayList<T>();
            for(String json:list){
                T t = JSON.parseObject(json, clazz);
                tList.add(t);
            }
            return tList;
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            closeConn(jedis);
        }
        return null;
    }
   
   

    /*****
     * Add new jedis list
     * @param key
     * @param list
     * @param time
     */
    public void setList(String key,List<T> list,Integer time){
        if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
            return ;
        }
        Jedis jedis = null;
        try{
            jedis = getConn();
            if(jedis.exists(key)){
            	/****
            	 * key is exist,so we just moved on
            	 */
            	return ;
            }
            for(T o:list){
                jedis.rpush(key,JSON.toJSONString(o));
            }
            if(time != null && time > 0){
                jedis.expire(key,time);
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            closeConn(jedis);
        }
    }

    
    /*****
     * Add new jedis list,Use transaction
     * @param key
     * @param list
     * @param time
     */
    public void setListUseTransaction(String key,List<T> list,Integer time){
        if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
            return ;
        }
        Jedis jedis = null;
        Transaction transaction = null;
        try{
            jedis = getConn();
            if(jedis.exists(key)){
            	/****
            	 * key is exist,so we just moved on
            	 */
            	return ;
            }
            transaction = jedis.multi();
            for(T o:list){
            	transaction.rpush(key,JSON.toJSONString(o));
            }
            if(time != null && time > 0){
            	transaction.expire(key,time);
            }
            transaction.exec();
        } catch (Exception e){
            e.printStackTrace();
            transaction.discard();
        } finally {
            closeConn(jedis);
            transaction = null;
        }
    }
    
    /****
     * replace old list in redis
     * @param key
     * @param list
     * @param time set expire property
     */
    public void replaceList(String key,List<T> list,Integer time){
        if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
            return ;
        }
        Jedis jedis = null;
        try{
            jedis = getConn();
            jedis.del(key);
                /****
                 * failed on delete exist key
                 */
               Long oldLength = jedis.llen(key);
                if(null != oldLength  && oldLength > 0){
                    /***
                     * key is exists,use the newList to replace the oldList
                     * first shold compare the size of those list
                     */
                    if(oldLength < list.size()){
                        /***
                         * means to add new value to jedis
                         */
                        for(int i = 0 ; i< list.size() ; i++){
                            if(i < oldLength){
                                jedis.lset(key, i, JSON.toJSONString(list.get(i)));
                            }else{
                                jedis.rpush(key,  JSON.toJSONString(list.get(i)));
                            }
                        }
                    }else{
                        /***
                         * means to delete value to jedis
                         */
                        for(int i = 0 ; i< oldLength ; i++){
                            if(i < list.size()){
                                jedis.lset(key, i, JSON.toJSONString(list.get(i)));
                            }else{
                                jedis.rpop(key);
                            }
                        }
                    }
                }else{
                    /****
                     * Key is deleted ,just rpush all the value
                     */
                    for(int i = 0 ; i < list.size() ; i++){
                        jedis.rpush(key,  JSON.toJSONString(list.get(i)));
                    }
                }
            if(time != null && time > 0){
                jedis.expire(key,time);
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            closeConn(jedis);
        }
    }


    /****
     * replace old list in redis,use transaction
     * @param key
     * @param list
     * @param time set expire property
     */
    public void replaceListUseTransaction(String key,List<T> list,Integer time){
        if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
            return ;
        }
        Jedis jedis = null;
        Transaction transaction = null;
        try{
            jedis = getConn();  
            jedis.del(key);
               Long oldLength = jedis.llen(key);
               jedis.watch(key);
               transaction = jedis.multi();
                if(null != oldLength  && oldLength > 0){
                    /***
                     * key is exists,use the newList to replace the oldList
                     * first shold compare the size of those list
                     */
                    if(oldLength < list.size()){
                        /***
                         * means to add new value to jedis
                         */
                        for(int i = 0 ; i< list.size() ; i++){
                            if(i < oldLength){
                            	transaction.lset(key, i, JSON.toJSONString(list.get(i)));
                            }else{
                            	transaction.rpush(key,  JSON.toJSONString(list.get(i)));
                            }
                        }
                    }else{
                        /***
                         * means to delete value to jedis
                         */
                        for(int i = 0 ; i< oldLength ; i++){
                            if(i < list.size()){
                            	transaction.lset(key, i, JSON.toJSONString(list.get(i)));
                            }else{
                            	transaction.rpop(key);
                            }
                        }
                    }
                }else{
                    /****
                     * Key is deleted ,just rpush all the value
                     */
                    for(int i = 0 ; i < list.size() ; i++){
                    	transaction.rpush(key,  JSON.toJSONString(list.get(i)));
                    }
                }
            if(time != null && time > 0){
            	transaction.expire(key,time);
            }
            transaction.exec();
        } catch (Exception e){
            e.printStackTrace();
            transaction.discard();
        } finally {
            closeConn(jedis);
            transaction = null;
        }
    }

    
    
    /****
     * Pipeline
     * Use for extremly large data set
     * @param key
     * @param list
     */
    public void setListUsePipeline(String key,List<T> list,Integer time){
        if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
            return ;
        }
        Jedis jedis = null;
        Pipeline pipeline = null;
        try{
            jedis = getConn();
            if(jedis.exists(key)){
            	/****
            	 * key is exist,so we just moved on
            	 */
            	return ;
            }
            pipeline = new Pipeline();
            pipeline.setClient(jedis.getClient());
            for(T o:list){
                pipeline.rpush(key,JSON.toJSONString(o));
            }
            /****
             * Set the expire property
             */
            if(time !=null && time > 0){
                pipeline.expire(key,time);
            }
            pipeline.sync();
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            closeConn(jedis);
        }
    }

    
    /****
     * Pipeline,Transaction
     * Use for extremly large data set
     * @param key
     * @param list
     */
    public void setListUsePipelineAndTransaction(String key,List<T> list,Integer time){
        if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
            return ;
        }
        Jedis jedis = null;
        Pipeline pipeline = null;
        try{
            jedis = getConn();
            if(jedis.exists(key)){
            	/****
            	 * key is exist,so we just moved on
            	 */
            	return ;
            }
            pipeline = new Pipeline();
            pipeline.setClient(jedis.getClient());
            pipeline.multi();
            for(T o:list){
                pipeline.rpush(key,JSON.toJSONString(o));
            }
            /****
             * Set the expire property
             */
            if(time !=null && time > 0){
                pipeline.expire(key,time);
            }
            pipeline.exec();
            pipeline.sync();
        }catch (Exception ex){
            ex.printStackTrace();
        }finally {
            closeConn(jedis);
        }
    }
    
    /****
     * This method is just use for large replace list
     * @param key
     * @param list
     */
    public void replaceListUsePipeline(String key,List<T> list,Integer time){
        if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
            return ;
        }
        Jedis jedis = null;
        Pipeline pipeline = null;
        try{
            jedis = getConn();
            jedis.del(key);
            /****
             * failed on delete exist key
             */
            Long oldLength = jedis.llen(key);
            pipeline = new Pipeline();
            pipeline.setClient(jedis.getClient());
            if(null != oldLength  && oldLength > 0){
                /***
                 * key is exists,use the newList to replace the oldList
                 * first shold compare the size of those list
                 */
                if(oldLength < list.size()){
                    /***
                     * means to add new value to jedis
                     */
                    for(int i = 0 ; i< list.size() ; i++){
                        if(i < oldLength){
                            pipeline.lset(key, i, JSON.toJSONString(list.get(i)));
                        }else{
                            pipeline.rpush(key,  JSON.toJSONString(list.get(i)));
                        }
                    }
                }else{
                    /***
                     * means to delete value to jedis
                     */
                    for(int i = 0 ; i< oldLength ; i++){
                        if(i < list.size()){
                            pipeline.lset(key, i, JSON.toJSONString(list.get(i)));
                        }else{
                            pipeline.rpop(key);
                        }
                    }
                }
            }else{
                /****
                 * Key is deleted ,just rpush all the value
                 */
                for(int i = 0 ; i < list.size() ; i++){
                    pipeline.rpush(key,JSON.toJSONString(list.get(i)));
                }
            }
            /****
             * Set the expire property
             */
            if(time != null && time > 0){
                pipeline.expire(key,time);
            }
            pipeline.sync();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            pipeline = null;
            closeConn(jedis);
        }
    }
    
    
    
    /****
     * Use pipelien And Transaction
     * This method is just use for large replace list
     * @param key
     * @param list
     */
    public void replaceListUsePipelineAndTransaction(String key,List<T> list,Integer time){
        if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
            return ;
        }
        Jedis jedis = null;
        Pipeline pipeline = null;
        try{
            jedis = getConn();
            jedis.del(key);
            Long oldLength = jedis.llen(key);
            pipeline = new Pipeline();
            pipeline.setClient(jedis.getClient());
            pipeline.multi();
            if(null != oldLength  && oldLength > 0){
                /***
                 * key is exists,use the newList to replace the oldList
                 * first shold compare the size of those list
                 */
                if(oldLength < list.size()){
                    /***
                     * means to add new value to jedis
                     */
                    for(int i = 0 ; i< list.size() ; i++){
                        if(i < oldLength){
                            pipeline.lset(key, i, JSON.toJSONString(list.get(i)));
                        }else{
                            pipeline.rpush(key,  JSON.toJSONString(list.get(i)));
                        }
                    }
                }else{
                    /***
                     * means to delete value to jedis
                     */
                    for(int i = 0 ; i< oldLength ; i++){
                        if(i < list.size()){
                            pipeline.lset(key, i, JSON.toJSONString(list.get(i)));
                        }else{
                            pipeline.rpop(key);
                        }
                    }
                }
            }else{
                /****
                 * Key is deleted ,just rpush all the value
                 */
                for(int i = 0 ; i < list.size() ; i++){
                    pipeline.rpush(key,JSON.toJSONString(list.get(i)));
                }
            }
            /****
             * Set the expire property
             */
            if(time != null && time > 0){
                pipeline.expire(key,time);
            }
            pipeline.exec();
            pipeline.sync();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            pipeline = null;
            closeConn(jedis);
        }
    }



    /****
     * append list to already existed redis list
     * @param key
     * @param list
     * @param time
     */
    public void appendList(String key,List<T> list,Integer time){
    	if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
            return ;
        }
        Jedis jedis = null;
        try{
            jedis = getConn();
            for(T o:list){
                jedis.rpush(key,JSON.toJSONString(o));
            }
            if(time != null && time > 0){
                jedis.expire(key,time);
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            closeConn(jedis);
        }
    }
    
    /****
     * append list to already existed redis list
     * @param key
     * @param list
     * @param time
     */
    public void appendListUseTransaction(String key,List<T> list,Integer time){
    	if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
            return ;
        }
        Jedis jedis = null;
        Transaction transaction = null;
        try{
            jedis = getConn();
            transaction = jedis.multi();
            for(T o:list){
            	transaction.rpush(key,JSON.toJSONString(o));
            }
            if(time != null && time > 0){
            	transaction.expire(key,time);
            }
            transaction.exec();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            closeConn(jedis);
        }
    }
    
    /****
     * append list to already existed redis list,Use pipeline
     * @param key
     * @param list
     * @param time
     */
    public void appendListUsePipeline(String key,List<T> list,Integer time){
    	if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
            return ;
        }
        Jedis jedis = null;
        Pipeline pipeline = null;
        try{
            jedis = getConn();
            pipeline = new Pipeline();
            pipeline.setClient(jedis.getClient());
            for(T o:list){
            	pipeline.rpush(key,JSON.toJSONString(o));
            }
            if(time != null && time > 0){
            	pipeline.expire(key,time);
            }
            pipeline.sync();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
        	pipeline = null;
            closeConn(jedis);
        }
    }
    
    
    /****
     * append list to already existed redis list,Use pipeline And Transaction
     * @param key
     * @param list
     * @param time
     */
    public void appendListUsePipelineAndTransaction(String key,List<T> list,Integer time){
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
            for(T o:list){
            	pipeline.rpush(key,JSON.toJSONString(o));
            }
            if(time != null && time > 0){
            	pipeline.expire(key,time);
            }
            pipeline.exec();
            pipeline.sync();
        } catch (Exception e){
            e.printStackTrace();
        } finally {
        	pipeline = null;
            closeConn(jedis);
        }
    }
    
    
    public void setList(String key,List<T> list){
    	setList(key, list, null);
    }
    
    public void setListUseTransaction(String key,List<T> list){
    	setListUseTransaction(key, list, null);
    }
    public void setListUsePipeline(String key,List<T> list){
    	setListUsePipeline(key, list, null);
    }
    public void setListUsePipelineAndTransaction(String key,List<T> list){
    	setListUsePipelineAndTransaction(key, list, null);
    }
    
    
    public void replaceList(String key,List<T> list){
    	replaceList(key, list, null);
    }
    
    public void replaceListUseTransaction(String key,List<T> list){
    	replaceListUseTransaction(key, list, null);
    }
    public void replaceListUsePipeline(String key,List<T> list){
    	replaceListUsePipeline(key, list, null);
    }
    public void replaceListUsePipelineAndTransaction(String key,List<T> list){
    	replaceListUsePipelineAndTransaction(key, list, null);
    }
    
    public void appendList(String key,List<T> list){
    	appendList(key, list, null);
    }
    
    public void appendListUseTransaction(String key,List<T> list){
    	appendListUseTransaction(key, list, null);
    }
    public void appendListUsePipeline(String key,List<T> list){
    	appendListUsePipeline(key, list, null);
    }
    public void appendListUsePipelineAndTransaction(String key,List<T> list){
    	appendListUsePipelineAndTransaction(key, list, null);
    }
    
}
