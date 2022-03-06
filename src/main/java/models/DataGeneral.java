package models;

import DAO.DataDao;
import org.apache.spark.sql.*;

import javax.xml.crypto.Data;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

public class DataGeneral implements DataDao {


    private SparkSession sparkSession;


    private final DataFrameReader dataframereader = sparkSession.read().option("header", true);

    public final Dataset<Row> data = dataframereader.csv("resources/wuzzuf_jobs.csv");


    public Dataset<Row> showData() {
        return data;
    }

    public Dataset<Row> getData(Dataset<Row> dataset) {
        return null;
    }

    @Override
    public String getStructure(Dataset<Row> dataset) {
        return dataset.schema().json();
    }

    @Override
    public Dataset<Row> getSummary(Dataset<Row> dataset) {
        return dataset.summary();
    }

    @Override
    public Dataset<Row> cleanData(Dataset<Row> dataset) {
        return dataset.dropDuplicates().summary();
    }

    @Override
    public Dataset<Row> jobsPerCompany(Dataset<Row> dataset) {
        return dataset.groupBy("Company").agg(count("Title")).orderBy(col("count(Title)").desc());
    }

    @Override
    public Dataset<Row> popularJobTitles(Dataset<Row> dataset) {
        return dataset.groupBy("Title").agg(count("Title")).orderBy(col("count(Title)").desc());
    }

    @Override
    public Dataset<Row> popularAreas(Dataset<Row> dataset) {
        return dataset.groupBy("Location").agg(count("Location")).orderBy(col("count(Location)").desc());
    }


    @Override
    public Dataset<Row> popularSkills(Dataset<Row> dataset) {
        List<String> allSkills = new ArrayList<String>();
        dataset.select("Skills").toLocalIterator().forEachRemaining(s -> {
            List<String> skills = Arrays.asList(s.getString(0).split(","));
            allSkills.addAll(skills);
        });
        Dataset<Row> Allskills = sparkSession.createDataset(allSkills, Encoders. STRING()).toDF("Skill");
        Allskills = Allskills.groupBy("Skill").agg(count("Skill")).orderBy(col("count(Skill)").desc());
        return Allskills;
    }
}
