package com.addthis.metrics3.reporter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.apache.cassandra.utils.EstimatedHistogram;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class CassandraReporter extends ScheduledReporter {

    public static final String SEPARATOR = ".";
    public static final String _50TH = SEPARATOR + "50th";
    public static final String _75TH = SEPARATOR + "75th";
    public static final String _95TH = SEPARATOR + "95th";
    public static final String _99TH = SEPARATOR + "99th";
    public static final String MAX = SEPARATOR + "max";
    public static final String MEAN = SEPARATOR + "mean";
    public static final String RATE = SEPARATOR + "rate";

    private static final Logger logger = LoggerFactory.getLogger(CassandraReporter.class);

    private final SimpleReporter reporter;

    private final Map<String, Long> countersMap = new HashMap<String, Long>();
    private final Predicate<String> metricFilter;

    public static String rateMetricName(String metric){
        return metric + RATE;
    }

    /**
     * Returns a new {@link Builder} for {@link CassandraReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link CassandraReporter}
     */
    public static CassandraReporter.Builder forRegistry(MetricRegistry registry) {
        return new CassandraReporter.Builder(registry);
    }

    /**
     * @param registry     metric registry to get metrics from
     * @param name         reporter name
     * @param filter       metric filter
     * @param rateUnit     unit for reporting rates
     * @param durationUnit unit for reporting durations
     * @see ScheduledReporter#ScheduledReporter(MetricRegistry, String, MetricFilter, TimeUnit, TimeUnit)
     */
    private CassandraReporter(MetricRegistry registry, String name, Predicate<String> metricFilter, SimpleReporter reporter, MetricFilter filter,
                              TimeUnit rateUnit, TimeUnit durationUnit) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.reporter = reporter;
        this.metricFilter = metricFilter;

        logger.info("Initialized NewRelicReporter for registry with name '{}', filter of type '{}', rate unit {} , duration unit {}",
                name, filter.getClass().getCanonicalName(), rateUnit.toString(), durationUnit.toString());
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges, SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms, SortedMap<String, Meter> meters, SortedMap<String, Timer> timers) {
        logger.debug("Received report of {} gauges, {} counters, {} histograms, {} meters and {} timers",
                gauges.size(), counters.size(), histograms.size(), meters.size(), timers.size());
        List<SimpleMetric> metrics = new ArrayList<SimpleMetric>();

        for (Map.Entry<String, Gauge> gaugeEntry : gauges.entrySet()) {
            metrics.addAll(gaugeMetrics(gaugeEntry.getKey(), gaugeEntry.getValue()));
        }

        for (Map.Entry<String, Histogram> histogramEntry : histograms.entrySet())
            metrics.addAll(histogramMetrics(histogramEntry.getKey(), histogramEntry.getValue()));

        for (Map.Entry<String, Counter> counterEntry : counters.entrySet())
            metrics.addAll(rateMetrics(counterEntry.getValue().getCount(), counterEntry.getKey()));

        for (Map.Entry<String, Meter> meterEntry : meters.entrySet())
            metrics.addAll(rateMetrics(meterEntry.getValue().getCount(), meterEntry.getKey()));

        for (Map.Entry<String, Timer> timerEntry : timers.entrySet())
            metrics.addAll(timerMetrics(timerEntry.getValue(), timerEntry.getKey()));

        for (SimpleMetric metric : metrics) {
            if (!shouldDrop(metric))
                recordMetric(metric);
            else
                logger.debug("Filtered metric {} with value {}", metric.getName(), metric.getValue());
        }


    }

    private boolean shouldDrop(SimpleMetric metric) {
        return metricFilter.test(metric.getName());
    }

    private List<SimpleMetric> rateMetrics(long count, String name) {
        Long acc = countersMap.get(name);
        countersMap.put(name, count);
        if (acc != null) {
            Long rate = count - acc;
            return Arrays.asList(metric(rate, name + RATE));
        }
        return Collections.emptyList();
    }

    private List<SimpleMetric> histogramMetrics(String name, Histogram histogram) {
        return snapshotMetrics(histogram.getSnapshot(), name, "");
    }

    private List<SimpleMetric> timerMetrics(Timer timer, String name) {
        List<SimpleMetric> m = new ArrayList<SimpleMetric>();
        m.addAll(snapshotMetrics(timer.getSnapshot(), name, "." + getDurationUnit()));
        m.addAll(rateMetrics(timer.getCount(), name));
        return m;
    }

    private List<SimpleMetric> snapshotMetrics(Snapshot snapshot, String name, String nameSuffix) {
        return Arrays.asList(
                //metric((float) convertDuration(snapshot.getMin()), name + ".min" + nameSuffix),
                metric((float) convertDuration(snapshot.getMax()), name + MAX + nameSuffix),
                metric((float) convertDuration(snapshot.getMean()), name + MEAN + nameSuffix),
                //metric((float) convertDuration(snapshot.getStdDev()), name + ".stdDev" + nameSuffix),
                metric((float) convertDuration(snapshot.getMedian()), name + _50TH + nameSuffix),
                metric((float) convertDuration(snapshot.get75thPercentile()), name + _75TH + nameSuffix),
                metric((float) convertDuration(snapshot.get95thPercentile()), name + _95TH + nameSuffix),
                //metric((float) convertDuration(snapshot.get98thPercentile()), name + ".98th" + nameSuffix),
                metric((float) convertDuration(snapshot.get99thPercentile()), name + _99TH + nameSuffix)
                //metric((float) convertDuration(snapshot.get999thPercentile()), name + ".999th" + nameSuffix)
        );
    }

    private SimpleMetric gaugeHistogram(String name, String percentileName, double percentile, EstimatedHistogram histogram) {
        return metric(histogram.percentile(percentile), name + percentileName);
    }

    private List<SimpleMetric> gaugeMetrics(String name, Gauge gauge) {
        Object gaugeValue = gauge.getValue();
        List<SimpleMetric> metrics = Collections.emptyList();

        if (gaugeValue instanceof Number) {
            float n = ((Number) gaugeValue).floatValue();
            if (!Float.isNaN(n) && !Float.isInfinite(n)) {
                metrics = Arrays.asList(metric(n, name));
            }
        }
        if (gaugeValue instanceof long[]) {
            long[] value = (long[]) gaugeValue;
            if (!ArrayUtils.isEmpty(value)) {
                EstimatedHistogram histogram = new EstimatedHistogram(value);
                if (histogram.isOverflowed()) {
                    logger.error(String.format("Row sizes are larger than %s, unable to calculate percentiles", histogram.getLargestBucketOffset()));
                } else {
                    metrics = Arrays.asList(
                            gaugeHistogram(name, _50TH, 0.5, histogram),
                            gaugeHistogram(name, _75TH, 0.75, histogram),
                            gaugeHistogram(name, _95TH, 0.95, histogram),
                            gaugeHistogram(name, _99TH, 0.99, histogram));
                }
            }
        }
        return metrics;
    }

    private SimpleMetric metric(float value, String rawName) {
        return new SimpleMetric(value, rawName);
    }

    private void recordMetric(SimpleMetric metric) {
        reporter.report(metric.getName(), metric.getValue());
    }


    public static final class Builder {
        private MetricRegistry registry;
        private String name;
        private MetricFilter filter;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private List<UnitConvertion> conversions;
        private SimpleReporter reporter;
        private Predicate<String> metricFilter;

        public Builder(MetricRegistry registry) {
            this.conversions = Collections.emptyList();
            this.registry = registry;
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.name = "new relic reporter";
            this.filter = MetricFilter.ALL;
            this.metricFilter = new Predicate<String>() {
                @Override
                public boolean test(String s) {
                    return false;
                }
            };
        }


        /**
         * @param name reporter name
         * @return this
         */
        public Builder name(String name) {
            this.name = name;
            return this;
        }

        /**
         * @param filter metric filter
         * @return this
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public Builder filter(List<String> rawPatterns, FilterColor color) {
            List<Pattern> patterns = new ArrayList<Pattern>();
            for (String p : rawPatterns) {
                try {
                    patterns.add(Pattern.compile(p));
                } catch (Exception e) {
                    logger.error("Pattern {} could not be compiled", p);
                }
            }
            this.metricFilter = new SimpleMetricFilter(patterns, color);
            return this;
        }

        public Builder conversions(List<UnitConvertion> conversions) {
            this.conversions = conversions;
            return this;
        }

        public Builder reporter(SimpleReporter reporter) {
            this.reporter = reporter;
            return this;
        }

        /**
         * @param rateUnit unit for reporting rates
         * @return this
         */
        public Builder rateUnit(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public Builder filter(Predicate<String> filter) {
            this.metricFilter = filter;
            return this;
        }

        /**
         * @param durationUnit unit for reporting durations
         * @return this
         */
        public Builder durationUnit(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }


        public CassandraReporter build() {
            return new CassandraReporter(registry, name, metricFilter, reporter, filter, rateUnit, durationUnit);
        }
    }
}