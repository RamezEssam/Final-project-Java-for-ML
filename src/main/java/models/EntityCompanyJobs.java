package models;

public class EntityCompanyJobs {
    private String company;
    private String jobCount;

    public EntityCompanyJobs(){}

    public EntityCompanyJobs(String company, String jobCount) {
        this.company = company;
        this.jobCount = jobCount;
    }

    public String getCompany() {
        return company;
    }

    public void setCompany(String company) {
        this.company = company;
    }

    public String getJobCount() {
        return jobCount;
    }

    public void setJobCount(String jobCount) {
        this.jobCount = jobCount;
    }
}
