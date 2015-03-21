package org.jerry.dao;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.jerry.model.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
/**
 * Administrator jiangshaowei
 * 2014Äê11ÔÂ21ÈÕ
 * kangjunlive8@hotmail.com
 */
@Component
public class PersonDao {
	
	@Autowired
	JdbcTemplate jdbcTemplate;

	private static final String QUERY_ALL_PERSONS="SELECT * from persons ";
	
	public List<Person> queryAllPersonList(){
		return jdbcTemplate.query(QUERY_ALL_PERSONS,new PersonRowMapper());		
	}
	
	class PersonRowMapper implements RowMapper<Person>{

		@Override
		public Person mapRow(ResultSet rs, int index) throws SQLException {
			Person person = new Person();
			try{
				person.setName(rs.getString("username"));
				person.setPassword(rs.getString("password"));
			}catch(Exception ex){
				ex.printStackTrace();
			}
			return person;
		}
		
	}
	
}
