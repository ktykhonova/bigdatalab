package workingWithKafka;

import java.util.Date;

public class UserVisitEntity {
    private String sourceIP;
    private String destURL;
    private String visitDate;
    private float adRevenue;
    private String userAgent;
    private String countryCode;
    private String languageCode;
    private String searchWord;
    private int duration;

    public UserVisitEntity(String sourceIP, String destURL, String visitDate, float adRevenue, String userAgent, String countryCode, String languageCode, String searchWord, int duration) {
        this.sourceIP = sourceIP;
        this.destURL = destURL;
        this.visitDate = visitDate;
        this.adRevenue = adRevenue;
        this.userAgent = userAgent;
        this.countryCode = countryCode;
        this.languageCode = languageCode;
        this.searchWord = searchWord;
        this.duration = duration;
    }

    public String getSourceIP() {

        return sourceIP;
    }

    public String getDestURL() {
        return destURL;
    }

    public String getVisitDate() {
        return visitDate;
    }

    public float getAdRevenue() {
        return adRevenue;
    }

    public String getUserAgent() {
        return userAgent;
    }

    public String getCountryCode() {
        return countryCode;
    }

    public String getLanguageCode() {
        return languageCode;
    }

    public String getSearchWord() {
        return searchWord;
    }

    public int getDuration() {
        return duration;
    }
}
