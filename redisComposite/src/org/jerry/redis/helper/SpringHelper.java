package org.jerry.redis.helper;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
public class SpringHelper
        implements ApplicationContextAware {

    private static ApplicationContext applicationContext ;


    public SpringHelper() {
    	
    }

    public void setApplicationContext(ApplicationContext context)
            throws BeansException {
        if(context!=null) applicationContext = context;
    }

    public static ApplicationContext getApplicationContext(){
        return applicationContext;
    }

    public static Object getBean(String beanName){
        return getApplicationContext().getBean(beanName);
    }
    
    public static void cleanHolder(){
        applicationContext = null;
    }

}


