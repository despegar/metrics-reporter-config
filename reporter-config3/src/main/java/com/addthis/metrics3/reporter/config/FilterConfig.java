package com.addthis.metrics3.reporter.config;

import java.util.List;

public class FilterConfig {

    private String color;

    private List<String> patterns;


    public String getColor() {
        return color;
    }

    public void setColor(String color) {
        this.color = color;
    }

    public List<String> getPatterns() {
        return patterns;
    }

    public void setPatterns(List<String> patterns) {
        this.patterns = patterns;
    }
}
