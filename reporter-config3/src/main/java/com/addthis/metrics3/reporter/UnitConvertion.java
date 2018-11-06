package com.addthis.metrics3.reporter;

public enum UnitConvertion {

    BYTES_TO_KB {
        @Override
        public float convert(float value) {
            return value / 1024;
        }

    };

    abstract float convert(float value);

    public static UnitConvertion fromString(String s){
        return UnitConvertion.valueOf(s.toUpperCase());
    }
}
