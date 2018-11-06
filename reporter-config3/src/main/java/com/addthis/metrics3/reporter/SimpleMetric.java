package com.addthis.metrics3.reporter;

public class SimpleMetric {

    private final float value;
    private final String name;

    public SimpleMetric(float value, String name){
        this.value = value;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public float getValue() {
        return value;
    }
}
