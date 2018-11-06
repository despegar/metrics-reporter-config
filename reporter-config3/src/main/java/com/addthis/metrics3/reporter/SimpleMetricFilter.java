package com.addthis.metrics3.reporter;

import java.util.List;
import java.util.regex.Pattern;

public class SimpleMetricFilter implements Predicate<String> {
    final FilterColor color;
    final PatternCache<Boolean> f;

    public SimpleMetricFilter(List<Pattern> patterns, FilterColor color) {
        this.color = color;
        this.f = PatternCache.matcher(patterns);
    }

    public boolean test(String value) {
        if (f.getOrElse(value,false)) return color.matched();
        else return color.unmatched();
    }

}
