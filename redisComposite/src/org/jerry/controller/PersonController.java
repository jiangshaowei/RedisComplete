package org.jerry.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.jerry.model.Person;
import org.jerry.service.PersonService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

@Controller("/person/")
public class PersonController {

	@Autowired
	PersonService personService;
	
	@RequestMapping(value= "/list",method = RequestMethod.POST)
	public void allPersonList(HttpServletRequest request, HttpServletResponse response){
		List<Person> persons = personService.queryAllPersons();
		for (Person person : persons) {
			System.out.println(person.getName());
		}
		
	}
	
}
