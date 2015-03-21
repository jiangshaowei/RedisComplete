package test;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.jerry.model.ListPerson;
import org.jerry.model.Person;
import org.jerry.redis.general.JedisList;
import org.jerry.redis.general.JedisMap;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by Administrator on 14-11-20.
 */
public class JedisTest {
	
	JedisMap<ListPerson> persons = new JedisMap<ListPerson>(ListPerson.class);
	private String key = "cache_persons_key";
	private String field1 = "123";
	private String field2 = "124";
	private ListPerson personsList ;
	@Before
	public void setup(){
		personsList = new ListPerson();
		Person p1 = new Person("bob", "1123");
		Person p2 = new Person("jerry","1234");
		List<Person> lists = new ArrayList<Person>();
		lists.add(p1);
		lists.add(p2);
		personsList.setPersons(lists);
		ListPerson plist = new ListPerson();
		plist.setPersons(new ArrayList<Person>());
		persons.setMapValue(key, field1, personsList);
		persons.setMapValue(key, field2,plist );
		
		
	}
	
    @Test
    public void testJedis(){
      /*  JedisObject<Person> personJedis = new JedisObject<Person>(Person.class);
        Person person = new Person("jiangshaowei","jiangshaowei");
        personJedis.set("myperson",  JSON.toJSONString(person));*/
    
    
    	ListPerson ps = persons.getMapValue(key, field2);
    	if(ps == null ){
    		System.out.println("Null");
    	}else{
    		System.out.println("Not Empty");
    	}

    }

    @Test
    public void testJedisPipeline(){
        List<Person> persons = new ArrayList<Person>( );
        for(int i = 0  ; i < 100000; i++){
            persons.add(new Person("cola","123456"));
        }
        String key = "persons:list";
        JedisList<Person> personList = new JedisList<Person>(Person.class);
        Timestamp start = new Timestamp(System.currentTimeMillis());
  //      personList.setListPipe("persons:list",persons,null);
 //       personList.setList("persons:list",persons,null);
        personList.replaceListUsePipeline(key, persons, null);
        Timestamp end = new Timestamp(System.currentTimeMillis());
        System.out.println(end.getTime() - start.getTime() );
    }

}
