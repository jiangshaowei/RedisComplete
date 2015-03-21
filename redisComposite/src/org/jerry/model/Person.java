package org.jerry.model;

/**
 * Created by Administrator on 14-11-20.
 */
public class Person {
    private String name;
    private String password;

    public Person(){

    }
    public Person(String name ,String password){
        this.name = name;
        this.password = password;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
