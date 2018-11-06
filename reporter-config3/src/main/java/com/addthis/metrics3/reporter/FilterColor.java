package com.addthis.metrics3.reporter;

public enum FilterColor {
    BLACK {
        @Override
        boolean matched() {
            return true;
        }

        @Override
        boolean unmatched() {
            return false;
        }
    },
    WHITE {
        @Override
        boolean matched() {
            return false;
        }

        @Override
        boolean unmatched() {
            return true;
        }
    };

    public static FilterColor fromString(String s) {
        return FilterColor.valueOf(s.toUpperCase());
    }

    /**
     *
     * @return true if the matched value should be dropped
     */
    abstract boolean matched();

    /**
     *
     * @return true if the unmatched value should be dropped
     */
    abstract boolean unmatched();


}