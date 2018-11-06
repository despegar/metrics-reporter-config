package com.addthis.metrics3.reporter;

import java.util.Map;
import java.util.regex.Pattern;

public class Converter {

    private final PatternCache<UnitConvertion> conversions;

    public Converter(Map<Pattern, UnitConvertion> conversions) {
        this.conversions = new PatternCache<UnitConvertion>(conversions);
    }

    public float convert(String name, float value) {
        UnitConvertion conv = conversions.get(name);
        if (conv == null) return value;
        else return conv.convert(value);
    }

}
