package org.jerry.service;

import java.util.List;

import org.jerry.dao.PersonDao;
import org.jerry.model.Person;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PersonService {

	@Autowired
	PersonDao personDao;
	
	public List<Person> queryAllPersons(){
		return personDao.queryAllPersonList();
	}
	
}
