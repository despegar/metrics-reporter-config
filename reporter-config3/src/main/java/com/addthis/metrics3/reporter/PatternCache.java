package com.addthis.metrics3.reporter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class PatternCache<T> {

    private class Entry {

        T value;

        public Entry(T value){
            this.value = value;
        }
    }

    final Map<Pattern,T> patterns;
    final Map<String, Entry> cache = new HashMap<String, Entry>();

    public PatternCache(Map<Pattern,T> patterns) {
        this.patterns = patterns;
    }

    public static PatternCache<Boolean> matcher(List<Pattern> patterns) {
        Map<Pattern,Boolean> map = new HashMap<Pattern,Boolean>();
        for (Pattern p: patterns){
            map.put(p,Boolean.TRUE);
        }
        return new PatternCache<Boolean>(map);
    }

    public T get(String value){
        Entry result = cache.get(value);
        if (result == null) {
            result = new Entry(getFromPatterns(value));
            cache.put(value, result);
        }
        return result.value;
    }

    public T getOrElse(String value, T orElse){
         T result = get(value);
         if (result == null) return orElse;
         else return result;
    }

    private T getFromPatterns(String value) {
        for (Map.Entry<Pattern, T> pattern : patterns.entrySet()) {
            if (pattern.getKey().matcher(value).matches()) return pattern.getValue();
        }
        return null;
    }

}
