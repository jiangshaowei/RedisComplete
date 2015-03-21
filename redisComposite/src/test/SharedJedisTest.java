package test;

import org.jerry.model.Person;
import org.jerry.redis.shared.ShardedJedisObject;
import org.junit.Test;

public class SharedJedisTest {


	@Test
	public void testShardedJedisObject(){
		ShardedJedisObject<Person> personObject = new ShardedJedisObject<Person>(Person.class);
		
		Person person = new Person("cola", "123456");
		personObject.set("mykey", person, null);
	//	Person p = personObject.get("mykey");
	//	p.getName();
		
		
	}
}
