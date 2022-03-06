package Spark.spring.Service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;

@Service
public class Services {
    @Autowired
    SparkSession sparkSession;
    //Dataset<Row> data = readata(sparkSession);

    public ResponseEntity<String> showData(Dataset<Row> data){
        // Dataset<Row> dataset = sparkSession.read().option("header", "true").csv("src/main/resources/pyramids.csv");
        // response = dataset.show();
        return ResponseEntity.ok(data.showString(20,5,true));
    }

    public ResponseEntity<String> printSchema(Dataset<Row> data){
//        return ResponseEntity.ok(data.toString());
        return ResponseEntity.ok(  data.schema().json());
    }

//    public Map<String,Integer> countJobs(Dataset<Row> data,String col){
//        data.groupBy(col).agg("count").select().map("count");
//    }
//    public ResponseEntity<String> chema(Dataset<Row> data){ //(Optional<Dataset<Row>>) (Object)
//        return ResponseEntity.ok(  data.schema().json());
//        //return  ResponseEntity.ok(data.schema());// .printSchema());
//    }



}




// Dataset<Row> dataset = sparkSession.read().option("header","true").csv("src/main/resources/pyramids.csv");


//    private Dataset<Row> readata(SparkSession sparkSession) {
//        return sparkSession.read().option("header","true").csv("src/main/resources/pyramids.csv");
//    }
//
//
