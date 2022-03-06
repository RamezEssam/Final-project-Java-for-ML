package Spark.spring;

import Spark.spring.Service.Services;
import models.DataGeneral;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

@RequestMapping("spark-context")
@Controller
public class SparkController {
//    @Autowired
//    private SparkSession sparkSession;
    @Autowired
    private final Services services;
    @Autowired
    private final Dataset<Row> data;

    private final DataGeneral dataGeneral;

    public SparkController(Services services, Dataset<Row> data, DataGeneral dataGeneral) {
        this.services = services;
        this.data = data;
//        data.printSchema();
        this.dataGeneral = dataGeneral;
    }
    // this rewquest prints the schema of the data already uploaded as bean
    @GetMapping("schema")
    public ResponseEntity<String> show(){
        //call method to visualize and save image
        return ResponseEntity.ok(dataGeneral.getData(data).toString());

    }

//    @GetMapping("test")
//    public ResponseEntity<String> test(){ //ResponseEntity<StructType>
//        return services.chema(data);
//    }


//    @GetMapping("read-csv")
//    public ResponseEntity<String> show(){
//
//        return services.showData(data);
//    }


//    public ResponseEntity<String> getRowCount() {
//        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/pyramids.csv");
//        String html = String.format("<h1>%s</h1>", "Running Apache Spark on/with support of Spring boot") +
//                String.format("<h2>%s</h2>", "Spark version = "+sparkSession.sparkContext().version()) +
//                String.format("<h3>%s</h3>", "Read csv..") +
//                String.format("<h4>Total records %d</h4>", dataset.count()) +
//                String.format("<h5>Schema <br/> %s</h5> <br/> Sample data - <br/>", dataset.schema().treeString()) +
//                dataset.showString(20, 20, true);
//        return ResponseEntity.ok(html);
//    }
//    public ResponseEntity<String> getRowcount(){
//        Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/pyramids.csv");
//        // response = dataset.show();
//        return ResponseEntity.ok(dataset.showString(20,5,true));
//    }

//    @GetMapping(path = "describe2")
//    public ResponseEntity<String> describe(){
//        Dataset<Row> data = sparkSession.read().option("header", "true").csv("src/main/resources/pyramids.csv");
//        return  ResponseEntity.ok(data.describe("Base1 (m)").toString());
//    }
}
