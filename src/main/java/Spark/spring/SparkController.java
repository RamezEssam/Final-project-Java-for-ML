package Spark.spring;

import models.DataGeneral;
import models.EntityCompanyJobs;
import models.EntityRow;
import models.EntitySummary;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import java.awt.*;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@RequestMapping("spark-context")
@Controller
public class SparkController {
    @Autowired
    SparkSession sparkSession;

    //@Autowired
    //private final Services services;
    @Autowired
    private final Dataset<Row> data;

    private final DataGeneral dataGeneral;

    private final DataFrameReader dataframereader = sparkSession.read().option("header", true);

    public final Dataset<Row> dataset = dataframereader.csv("resources/wuzzuf_jobs.csv");

    @Autowired
    public SparkController(Dataset<Row> data, DataGeneral dataGeneral) {
        //this.services = services;
        this.data = data;
//        data.printSchema();
        this.dataGeneral = dataGeneral;
    }
    // this request prints the schema of the data already uploaded as bean
    @GetMapping("show-data")
    public String show(Model model){
        Dataset<Row> dataSet = dataGeneral.getData(dataset);
        List<EntityRow> entityRows = new ArrayList<EntityRow>();
        dataSet.toLocalIterator().forEachRemaining(s->{
            entityRows.add(new EntityRow(
                    s.getString(0),
                    s.getString(1),
                    s.getString(2),
                    s.getString(3),
                    s.getString(4),
                    s.getString(5),
                    s.getString(6),
                    s.getString(7)
            ));
        });

        model.addAttribute("rows", entityRows);
        return "show-data/index";
    }

    @GetMapping("show-summary")
    public String showSummary(Model model){
        Dataset<Row> summary = dataGeneral.getSummary(dataset);
        List<EntitySummary> entitySummaries = new ArrayList<EntitySummary>();
        summary.toLocalIterator().forEachRemaining(s->{
            entitySummaries.add(new EntitySummary(
                    s.getString(0),
                    s.getString(1),
                    s.getString(2),
                    s.getString(3),
                    s.getString(4),
                    s.getString(5),
                    s.getString(6),
                    s.getString(7),
                    s.getString(8)
            ));
        });
        model.addAttribute("rows", entitySummaries);

        return "show-summary/index";
    }
    @GetMapping("clean-data")
    public String cleanData(Model model){
        Dataset<Row> cleanData = dataGeneral.cleanData(dataset);
        List<EntitySummary> entitySummaries = new ArrayList<EntitySummary>();
        cleanData.toLocalIterator().forEachRemaining(s->{
            entitySummaries.add(new EntitySummary(
                    s.getString(0),
                    s.getString(1),
                    s.getString(2),
                    s.getString(3),
                    s.getString(4),
                    s.getString(5),
                    s.getString(6),
                    s.getString(7),
                    s.getString(8)
            ));
        });
        model.addAttribute("rows", entitySummaries);

        return "clean-data/index";
    }
    @GetMapping("jobs-per-company")
    public String jobsPerCompany(Model model){
        Dataset<Row> jobsPerCompany = dataGeneral.jobsPerCompany(dataset);
        List<EntityCompanyJobs> entityCompanyJobs = new ArrayList<EntityCompanyJobs>();
        jobsPerCompany.toLocalIterator().forEachRemaining(s->{
            entityCompanyJobs.add(new EntityCompanyJobs(
                    s.getString(0),
                    s.getString(1)
            ));
        });

        model.addAttribute("rows", entityCompanyJobs);

        return "jobs-per-company/index";
    }

    @GetMapping("popular-job-titles")
    public String popularJobTitles(Model model){
        Dataset<Row> popularJobTitles = dataGeneral.popularJobTitles(dataset);
        List<EntityCompanyJobs> entityCompanyJobs = new ArrayList<EntityCompanyJobs>();
        popularJobTitles.toLocalIterator().forEachRemaining(s->{
            entityCompanyJobs.add(new EntityCompanyJobs(
                    s.getString(0),
                    s.getString(1)
            ));
        });

        model.addAttribute("rows", entityCompanyJobs);

        return "popular-job-titles/index";
    }

    @GetMapping("popular-areas")
    public String popularAreas(Model model){
        Dataset<Row> popularAreas = dataGeneral.popularAreas(dataset);
        List<EntityCompanyJobs> entityCompanyJobs = new ArrayList<EntityCompanyJobs>();
        popularAreas.toLocalIterator().forEachRemaining(s->{
            entityCompanyJobs.add(new EntityCompanyJobs(
                    s.getString(0),
                    s.getString(1)
            ));
        });

        model.addAttribute("rows", entityCompanyJobs);

        return "popular-areas/index";
    }

    @GetMapping("popular-skills")
    public String popularSkills(Model model){
        Dataset<Row> popularSkills = dataGeneral.popularSkills(dataset);
        List<EntityCompanyJobs> entityCompanyJobs = new ArrayList<EntityCompanyJobs>();
        popularSkills.toLocalIterator().forEachRemaining(s->{
            entityCompanyJobs.add(new EntityCompanyJobs(
                    s.getString(0),
                    s.getString(1)
            ));
        });

        model.addAttribute("rows", entityCompanyJobs);

        return "popular-skills/index";
    }

    @GetMapping("jobs-per-company-pichart")
    public String drawPieChartJobsPercompany(Model model) throws IOException {

        // Create Chart
        PieChart chart = new PieChartBuilder().width(800).height(600).title(getClass().getSimpleName()).build();

        // Customize Chart
        Color[] sliceColors = new Color[] { new Color(224, 68, 14), new Color(230, 105, 62), new Color(236, 143, 110), new Color(243, 180, 159), new Color(246, 199, 182) };
        chart.getStyler().setSeriesColors(sliceColors);

        DataGeneral df = new DataGeneral();

        Dataset<Row> jobsPerCompany =  df.jobsPerCompany(dataset);


        final int[] i = {0};
        jobsPerCompany.toLocalIterator().forEachRemaining(s-> {
            if(i[0] < 7){chart.addSeries(s.getString(0), s.getLong(1));}
            i[0] += 1;

        });

        BitmapEncoder.saveBitmap(chart, "src/main/resources/static/JobsPerCompany", BitmapEncoder.BitmapFormat.JPG);

        return "jobs-per-company-pichart/index";
    }

    @GetMapping("popular-job-titles-barchart")
    public String drawBarChartPopularJobTitles(Model model) throws IOException {
        // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title("Popular Job Titles").xAxisTitle("Titles").yAxisTitle("Count").build();

        // Customize Chart
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNE);
        chart.getStyler().setXAxisLabelRotation(45);

        DataGeneral df = new DataGeneral();
        Dataset<Row> popularTitles = df.popularJobTitles(dataset);
        List<String> titlesList = new ArrayList<String>();
        final int[] i = {0};
        popularTitles.toLocalIterator().forEachRemaining(s->{
            if(i[0] < 10){titlesList.add(s.getString(0));}
            i[0] += 1;

        });

        List<Long> countList = new ArrayList<Long>();
        final int[] j = {0};
        popularTitles.toLocalIterator().forEachRemaining(s->{
            if(j[0] < 10){countList.add(s.getLong(1));}
            j[0] += 1;

        });
        // Series
        chart.addSeries("Job Title Count", titlesList, countList);

        BitmapEncoder.saveBitmap(chart, "src/main/resources/static/PopularTitles", BitmapEncoder.BitmapFormat.JPG);

        return "popular-job-titles-barchart/index";
    }


    @GetMapping("popular-areas-barchart")
    public String drawBarChartPopularAreas(Model model) throws IOException {

        // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title("Popular Areas").xAxisTitle("Areas").yAxisTitle("Count").build();

        // Customize Chart
        chart.getStyler().setLegendPosition(Styler.LegendPosition.InsideNE);
        chart.getStyler().setXAxisLabelRotation(45);

        DataGeneral df = new DataGeneral();
        Dataset<Row> popularAreas = df.popularAreas(dataset);
        List<String> areasList = new ArrayList<String>();
        final int[] i = {0};
        popularAreas.toLocalIterator().forEachRemaining(s->{
            if(i[0] < 10){areasList.add(s.getString(0));}
            i[0] += 1;

        });

        List<Long> countList = new ArrayList<Long>();
        final int[] j = {0};
        popularAreas.toLocalIterator().forEachRemaining(s->{
            if(j[0] < 10){countList.add(s.getLong(1));}
            j[0] += 1;

        });
        // Series
        chart.addSeries("Areas Count", areasList, countList);

        BitmapEncoder.saveBitmap(chart, "src/main/resources/static/PopularAreas", BitmapEncoder.BitmapFormat.JPG);

        return "popular-areas-barchart/index";
    }





}
