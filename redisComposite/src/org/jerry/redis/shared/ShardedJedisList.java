package org.jerry.redis.shared;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jerry.redis.shared.pool.SharedPool;

import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPipeline;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;

public class ShardedJedisList<T extends Object> extends SharedPool{

	
		private Class<T> clazz;
	    
	    /****
	     * in some case,we want store the list as map , we only let the client to give us key ,
	     * we don't want out user give us the field value ,so this LIST_STORE_AS_MAP_FIELD is
	     * the basic the field for our map
	     */
	    private static final String LIST_STORE_AS_MAP_FIELD = "list:map:";
	    public ShardedJedisList(Class<T> clazz){
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
	     * return jedis list, begin start to end
	     * @param key
	     * @param start
	     * @param end
	     * @return
	     */
	    public List<T> getList(String key,int start, int end){
	        if(StringUtils.isEmpty(key)){
	            return null;
	        }
	        ShardedJedis jedis = null;
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
	        ShardedJedis  jedis = null;
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
	     * replace old list in redis
	     * @param key
	     * @param list
	     * @param time set expire property
	     */
	    public void replaceList(String key,List<T> list,Integer time){
	        if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
	            return ;
	        }
	        ShardedJedis  jedis = null;
	        try{
	            jedis = getConn();
	            jedis.del(key);
	             
	               Long oldLength = jedis.llen(key);
	                if(null != oldLength  && oldLength > 0){
	                    /***
	                     * failed on delete exist key
	                     * key is exists,use the newList to replace the oldList
	                     * first should compare the size of those list
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
	     * Pipeline
	     * Use for extremly large data set
	     * @param key
	     * @param list
	     */
	    public void setListPipe(String key,List<T> list,Integer time){
	        if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
	            return ;
	        }
	        ShardedJedis jedis = null;
	        ShardedJedisPipeline pipeline = null;
	        try{
	            jedis = getConn();
	            pipeline = new ShardedJedisPipeline();
	            pipeline.setShardedJedis(jedis);
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
	     * This method is just use for large replace list
	     * @param key
	     * @param list
	     */
	    public void replaceListPipe(String key,List<T> list,Integer time){
	        if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
	            return ;
	        }
	        ShardedJedis jedis = null;
	        ShardedJedisPipeline pipeline = null;
	        try{
	            jedis = getConn();
	            jedis.del(key);
	            /****
	             * failed on delete exist key
	             */
	            Long oldLength = jedis.llen(key);
	            pipeline = new ShardedJedisPipeline();
	            pipeline.setShardedJedis(jedis);
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
	     * In Some case ,we don't need to get the sublist of our list 
	     * We just want store list in redis And always get all of them
	     * This case ,we might want to store list as map for both memory And I/O speed reason
	     * @param key
	     * @param list
	     * @param expireSecs
	     */
	    public void setListAsMap(String key,List<T> list, Integer expireSecs){
	    	if(StringUtils.isEmpty(key) || list == null || list.isEmpty()){
	    		return;
	    	}
	    	ShardedJedis jedis = null;
	    	try{
	    		jedis = getConn();
	    		jedis.hset(key, LIST_STORE_AS_MAP_FIELD + key , JSONArray.toJSONString(list));
	    		if(expireSecs != null && expireSecs > 0){
	    			jedis.expire(key, expireSecs);
	    		}
	    	}catch(Exception ex){
	    		ex.printStackTrace();
	    	}finally{
	    		closeConn(jedis);
	    	}
	    }
	    
	    /****
	     * Return the list previous we stored in redis map
	     * @param key
	     * @return
	     */
	    public List<T> getListThroughMap(String key){
	    	if(StringUtils.isEmpty(key)){
	    		return null;
	    	}
	    	ShardedJedis jedis = null;
	    	try{
	    		jedis = getConn();
	    		String object = jedis.hget(key, LIST_STORE_AS_MAP_FIELD + key);
	    		return StringUtils.isEmpty(object) ? null : JSONArray.parseArray(object, clazz);
	    	}catch(Exception ex){
	    		ex.printStackTrace();
	    	}finally{
	    		closeConn(jedis);
	    	}
	    	return null;
	    }
}
