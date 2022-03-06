package DAO;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DataDao {



    Dataset<Row> getData(Dataset<Row> dataset);

    String getStructure(Dataset<Row> dataset);

    Dataset<Row> getSummary(Dataset<Row> dataset);

    Dataset<Row> cleanData(Dataset<Row> dataset);

    Dataset<Row> jobsPerCompany(Dataset<Row> dataset);

    Dataset<Row> popularJobTitles(Dataset<Row> dataset);

    Dataset<Row> popularAreas(Dataset<Row> dataset);

    Dataset<Row> popularSkills(Dataset<Row> dataset);
}
