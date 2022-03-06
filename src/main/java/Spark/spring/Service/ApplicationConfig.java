package Spark.spring.Service;

import Spark.spring.SparkController;
import models.DataGeneral;
import models.EntityCompanyJobs;
import models.EntityRow;
import models.EntitySummary;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

@Configuration
@PropertySource("classpath:application.properties")
public class ApplicationConfig {

    @Value("${app.name:spark-sprint-boot}")
    private String appName;

    @Value("${master.uri:local}")
    private String masterUri;

    @Bean
    public SparkConf sparkConf() {
        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster(masterUri);

        return sparkConf;
    }

    @Bean
    public JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }

    @Bean
    public SparkSession sparkSession() {
        return SparkSession
                .builder()
                .sparkContext(javaSparkContext().sc())
                .appName("Integrating Spring-boot with Apache Spark")
                .getOrCreate();
    }
    @Bean
    public Dataset<Row> readata(SparkSession sparkSession){
        Dataset<Row> dataset = sparkSession.read().option("header","true").csv("src/main/resources/Wuzzuf_Jobs.csv");
        return dataset;
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }



    @Bean
    public DataGeneral dataStart(){
        return new DataGeneral();
    }

    /*@Bean
    public EntityRow entityRow(){
        return new EntityRow();
    }*/

    /*@Bean
    public EntitySummary entitySummary(){
        return new EntitySummary();
    }*/

    /*@Bean
    EntityCompanyJobs entityCompanyJobs(){
        return new EntityCompanyJobs();
    }*/
}