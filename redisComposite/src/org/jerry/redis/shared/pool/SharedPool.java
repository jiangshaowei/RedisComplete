package org.jerry.redis.shared.pool;

import java.util.ArrayList;
import java.util.List;

import org.jerry.redis.helper.SpringHelper;

import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;

public class SharedPool implements SharedInfoConstant{ 
	
	
	private ShardedJedisPool pool;
	public List<JedisShardInfo> shardedInfo ;
	
	/****
	 * Return pool resources also close the sharedjedis
	 * @param jedis
	 */
	public void closeConn(ShardedJedis jedis){
		if(jedis != null){
			pool.returnResource(jedis);
			jedis.disconnect();
			jedis = null ;
		}
	}
	
	
	/*****
	 * return sharedjedis from the shardedJedisPool
	 * @return
	 */
	public ShardedJedis getConn(){
		if(pool == null ){
		   /* JedisPoolConfig config = new JedisPoolConfig();
		    config.setMaxActive(200);
		    config.setMaxWait(1000);
		    config.setMaxIdle(10);
		    if(shardedInfo == null || shardedInfo.isEmpty()){
		    	initialize();
		    }
		    pool = new ShardedJedisPool(config, shardedInfo);
		    */
			pool = (ShardedJedisPool)SpringHelper.getBean("shardedJedisPool");
		}
		
		return pool.getResource();
		
	}


	/****
	 * Generate ShardedJedis Info
	 */
	@Deprecated
	private void initialize() {
		    shardedInfo = new ArrayList<JedisShardInfo>();
			JedisShardInfo sharInfoOne = new JedisShardInfo(HOST_ONE,HOST_ONE_PORT,2000,1);
			JedisShardInfo sharInfoTwo = new JedisShardInfo(HOST_TWO, HOST_TWO_PORT, 2000 , 10);
			JedisShardInfo sharInfoThree = new JedisShardInfo(HOST_THREE, HOST_THREE_PORT, 2000 , 1);
			shardedInfo.add(sharInfoOne);
			shardedInfo.add(sharInfoTwo);
			shardedInfo.add(sharInfoThree);
	}
	
}
