package org.jerry.redis.pubsub;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;

public class Subsribe extends org.jerry.redis.general.JedisConn{

	
	/****
	 * Channel subsribe
	 * @param channels
	 */
	public void subsribe(String... channels){
		if(channels == null || channels.length == 0){
			return;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			jedis.subscribe(new ChannelJedisPubSub(),channels);
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
	}

	/****
	 * Pattern subsribe
	 * @param patterns
	 */
	public void psubsribe(String... patterns){
		if(patterns == null || patterns.length == 0){
			return ;
		}
		Jedis jedis = null;
		try{
			jedis = getConn();
			jedis.psubscribe(new ChannelJedisPubSub(), patterns);
		}catch(Exception ex){
			ex.printStackTrace();
		}finally{
			closeConn(jedis);
		}
	}
	
	
	
	class ChannelJedisPubSub extends JedisPubSub{

		@Override
		public void onMessage(String strChannel, String strMessage) {
			// TODO Auto-generated method stub
 			System.out.println("Gotta Message :" + strMessage + " From channel :" + strChannel);
		}

		@Override
		public void onPMessage(String strPattern, String strChannel,
				String strMessage) {
			// TODO Auto-generated method stub
			System.out.println("Gotta Message :" + strMessage + " From channel :" + strChannel + " Which pattern is :" + strPattern);
		}

		@Override
		public void onSubscribe(String strChannel, int subscribedChannels) {
			// TODO Auto-generated method stub
			System.out.println("Subsribe :" + strChannel + " " + subscribedChannels + "");
			
		}

		@Override
		public void onUnsubscribe(String strChannel, int subscribedChannels) {
			// TODO Auto-generated method stub
			System.out.println("Unsubsribe : " + strChannel + " " + subscribedChannels + "");
		}

		@Override
		public void onPUnsubscribe(String strPattern, int subscribedChannels) {
			// TODO Auto-generated method stub
			System.out.println("Unpsubsribe : " + strPattern + " " + subscribedChannels + "");
		}

		@Override
		public void onPSubscribe(String strPattern, int subscribedChannels) {
			// TODO Auto-generated method stub
			System.out.println("PSubsribe : " + strPattern + " " + subscribedChannels + "");
		}
		
	}
	
	public static void main(String[] args){
		Subsribe sub = new Subsribe();
		String[] patterns = new String[]{"system.all"};
	    sub.psubsribe(patterns);
		
	}
}
