package org.jerry.redis.pubsub;

import org.apache.commons.lang.StringUtils;

import redis.clients.jedis.Jedis;

public class Publish extends org.jerry.redis.general.JedisConn{

	
	
	/****
	 * Publish message to specific channel
	 * @param channel
	 * @param message
	 */
	public void publish(String channel,String message){
		if(StringUtils.isEmpty(channel) || StringUtils.isEmpty(message)){
			return;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			jedis.publish(channel, message);
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
	}
	
	public static void main(String[] args){
		Publish pub = new Publish();
		pub.publish("channel:1000", "This is subsribe");
	}
}
