package models;

public class EntitySummary {
    private String summary;
    private String title;
    private String company;
    private String location;
    private String type;
    private String level;
    private String yearsExp;
    private String country;
    private String skills;

    public EntitySummary() {}

    public EntitySummary(String summary, String title, String company, String location, String type, String level, String yearsExp, String country, String skills) {
        this.summary = summary;
        this.title = title;
        this.company = company;
        this.location = location;
        this.type = type;
        this.level = level;
        this.yearsExp = yearsExp;
        this.country = country;
        this.skills = skills;
    }
}
