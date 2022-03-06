package Spark;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.ApplicationContext;

@SpringBootApplication
public class Main extends SpringBootServletInitializer {
    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringApplication.run(Main.class, args);
    }
}
