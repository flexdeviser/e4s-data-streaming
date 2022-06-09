package org.e4s.app;

import java.util.Calendar;
import java.util.TimeZone;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class AppLauncher {

    private Logger LOG = LoggerFactory.getLogger(AppLauncher.class);

    public static void main(String[] args) {
        SpringApplication.run(AppLauncher.class, args);
    }

    @PostConstruct
    public void init(){
        // Setting Spring Boot SetTimeZone
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        final Calendar calendar = Calendar.getInstance();
        LOG.info("Application running with TZ: {}", calendar.getTimeZone().getID());
    }
}
