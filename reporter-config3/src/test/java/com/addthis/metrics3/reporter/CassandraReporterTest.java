package com.addthis.metrics3.reporter;

import java.io.OutputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;

public class CassandraReporterTest {

    final String GAUGE = "Gauge";
    final String TIMER = "Timer";
    final String HISTOGRAM = "Histogram";
    final String COUNTER = "Counter";
    final String METER = "Meter";
    final Snapshot snapshot = snapshot(5000, 200.4, new double[]{123000, 150000, 300000, 456000, 567000, 888000});
    final float[] percentiles = new float[]{123f, 150f, 300f, 567f};
    final TimeUnit DURATION_UNIT = TimeUnit.MILLISECONDS;
    final String TIMER_SUFFIX = "." + DURATION_UNIT.toString().toLowerCase();

    MetricRegistry registry;
    SimpleReportertTest internal;
    CassandraReporter reporter;

    @Before
    public void initializeReporter() {
        registry = new MetricRegistry();
        internal = new SimpleReportertTest();
        reporter = CassandraReporter.forRegistry(registry)
                .reporter(internal)
                .durationUnit(DURATION_UNIT)
                .build();
    }

    class MetricsBuilder {
        private final SortedMap<String, Meter> meters = new TreeMap<String, Meter>();
        private final SortedMap<String, Counter> counters = new TreeMap<String, Counter>();
        private final SortedMap<String, Timer> timers = new TreeMap<String, Timer>();
        private final SortedMap<String, Gauge> gauges = new TreeMap<String, Gauge>();
        private final SortedMap<String, Histogram> histograms = new TreeMap<String, Histogram>();
        private final CassandraReporter reporter;

        public MetricsBuilder(CassandraReporter reporter) {
            this.reporter = reporter;
        }

        public MetricsBuilder gauge(final Object gauge) {
            return gauge(GAUGE, gauge);
        }

        public MetricsBuilder gauge(String name, final Object gauge) {
            gauges.put(name, new Gauge() {
                @Override
                public Object getValue() {
                    return gauge;
                }
            });
            return this;
        }

        public MetricsBuilder histogram(final Snapshot snapshot) {
            return this.histogram(HISTOGRAM, snapshot);
        }

        public MetricsBuilder histogram(String name, final Snapshot snapshot) {
            histograms.put(name, new Histogram(null) {
                @Override
                public Snapshot getSnapshot() {
                    return snapshot;
                }
            });
            return this;
        }

        public MetricsBuilder meter(long marks) {
            Meter meter = new Meter();
            meter.mark(marks);
            meters.put(METER, meter);
            return this;
        }

        public MetricsBuilder timer(final Snapshot snapshot, final long count) {
            return timer(TIMER, snapshot, count);
        }

        public MetricsBuilder timer(String name, final Snapshot snapshot, final long count) {
            timers.put(name, new Timer() {
                @Override
                public Snapshot getSnapshot() {
                    return snapshot;
                }

                @Override
                public long getCount() {
                    return count;
                }
            });
            return this;
        }

        public MetricsBuilder counter(long count) {
            return this.counter(COUNTER, count);
        }

        public MetricsBuilder counter(String name, long count) {
            Counter c = new Counter();
            c.inc(count);
            counters.put(name, c);
            return this;
        }

        public SortedMap<String, Counter> getCounters() {
            return counters;
        }

        public SortedMap<String, Gauge> getGauges() {
            return gauges;
        }

        public SortedMap<String, Meter> getMeters() {
            return meters;
        }

        public SortedMap<String, Timer> getTimers() {
            return timers;
        }

        public SortedMap<String, Histogram> getHistograms() {
            return histograms;
        }

        public void report() {
            reporter.report(getGauges(), getCounters(), getHistograms(), getMeters(), getTimers());
            getGauges().clear();
            getCounters().clear();
            getHistograms().clear();
            getMeters().clear();
            getTimers().clear();
        }

    }

    class SimpleReportertTest implements SimpleReporter {

        final Map<String, Float> metrics = new HashMap<String, Float>();

        @Override
        public void report(String name, float value) {
            metrics.put(name, value);
        }

        public Map<String, Float> getMetrics() {
            return metrics;
        }

        public Float getMetric(String name) {
            return getMetrics().get(name);
        }

        public SimpleReportertTest checkMetric(String metricName, Float value) {
            Assert.assertEquals(value, internal.getMetric(metricName));
            return this;
        }

        public SimpleReportertTest checkHistrogramPercentiles(String histogram, float[] percentiles) {
            return checkMetric(histogram + CassandraReporter._50TH, percentiles[0])
                    .checkMetric(histogram + CassandraReporter._75TH, percentiles[1])
                    .checkMetric(histogram + CassandraReporter._95TH, percentiles[2])
                    .checkMetric(histogram + CassandraReporter._99TH, percentiles[3]);
        }

        public SimpleReportertTest checkHistrogramPercentiles(float[] percentiles) {
            return checkHistrogramPercentiles(HISTOGRAM, percentiles);
        }

        public SimpleReportertTest checkTimerPercentiles(String timer, float[] percentiles) {
            return checkMetric(timer + CassandraReporter._50TH + TIMER_SUFFIX, percentiles[0])
                    .checkMetric(timer + CassandraReporter._75TH + TIMER_SUFFIX, percentiles[1])
                    .checkMetric(timer + CassandraReporter._95TH + TIMER_SUFFIX, percentiles[2])
                    .checkMetric(timer + CassandraReporter._99TH + TIMER_SUFFIX, percentiles[3]);
        }

        public SimpleReportertTest checkTimerPercentiles(float[] percentiles) {
            return checkTimerPercentiles(TIMER, percentiles);
        }

        public SimpleReportertTest checkUnreportedMetric(String name) {
            return checkMetric(name, null);
        }

        public SimpleReportertTest checkGauge(Float value) {
            return checkMetric(GAUGE, value);
        }

        public SimpleReportertTest checkRate(String metric, float value) {
            return checkMetric(metric + CassandraReporter.RATE, value);
        }

        public SimpleReportertTest checkCounterRate(float value) {
            return checkRate(COUNTER, value);
        }

        public SimpleReportertTest checkMeterRate(float value) {
            return checkRate(METER, value);
        }

        public SimpleReportertTest checkTimerRate(float value) {
            return checkRate(TIMER, value);
        }

        public SimpleReportertTest clear() {
            this.metrics.clear();
            return this;
        }

    }

    public Snapshot snapshot(final long max, final double mean, final double[] percentiles) {
        return new Snapshot() {

            private long millisToNanos(double millis) {
                return TimeUnit.MICROSECONDS.toNanos(Double.valueOf(millis).longValue());
            }

            @Override
            public double getValue(double quantile) {
                switch (Double.valueOf(quantile * 1000).intValue()) {
                    case 500:
                        return millisToNanos(percentiles[0]);
                    case 750:
                        return millisToNanos(percentiles[1]);
                    case 950:
                        return millisToNanos(percentiles[2]);
                    case 980:
                        return millisToNanos(percentiles[3]);
                    case 990:
                        return millisToNanos(percentiles[4]);
                    case 999:
                        return millisToNanos(percentiles[5]);
                    default:
                        throw new IllegalStateException();
                }
            }

            @Override
            public long[] getValues() {
                throw new IllegalStateException();
            }

            @Override
            public int size() {
                throw new IllegalStateException();
            }

            @Override
            public long getMax() {
                return max;
            }

            @Override
            public double getMean() {
                return mean;
            }

            @Override
            public long getMin() {
                throw new IllegalStateException();
            }

            @Override
            public double getStdDev() {
                throw new IllegalStateException();
            }

            @Override
            public void dump(OutputStream output) {

            }
        };
    }

    @Test
    public void testGauge() {
        new MetricsBuilder(reporter).gauge(1f).report();
        internal.checkGauge(1f);
    }

    @Test
    public void testGauges() {
        new MetricsBuilder(reporter)
                .gauge(1f)
                .gauge("gauge2", 7f)
                .report();
        internal.checkGauge(1f)
                .checkMetric("gauge2", 7f);
    }

    @Test
    public void testTimer() {
        new MetricsBuilder(reporter)
                .timer(snapshot, 15000)
                .timer("timer2", snapshot, 123)
                .report();
        internal.checkTimerPercentiles(percentiles)
                .checkTimerPercentiles("timer2", percentiles);
    }

    @Test
    public void testHistograms() {
        new MetricsBuilder(reporter)
                .histogram(snapshot)
                .histogram("histogram2", snapshot)
                .report();
        internal.checkHistrogramPercentiles(percentiles)
                .checkHistrogramPercentiles("histogram2", percentiles);
    }

    @Test
    public void testAll() {
        MetricsBuilder builder = new MetricsBuilder(reporter);

        builder.timer(snapshot, 277)
                .gauge(5)
                .histogram(snapshot)
                .counter(5)
                .meter(100)
                .report();
        internal.checkGauge(5f)
                .checkHistrogramPercentiles(percentiles)
                .checkTimerPercentiles(percentiles)
                .checkUnreportedMetric(CassandraReporter.rateMetricName(TIMER))
                .checkUnreportedMetric(CassandraReporter.rateMetricName(COUNTER))
                .checkUnreportedMetric(CassandraReporter.rateMetricName(METER))
                .clear();

        builder.timer(snapshot, 277)
                .gauge(8)
                .counter(12)
                .meter(250)
                .report();
        internal.checkGauge(8f)
                .checkUnreportedMetric(HISTOGRAM + CassandraReporter._75TH)
                .checkTimerPercentiles(percentiles)
                .checkTimerRate(0)
                .checkCounterRate(7)
                .checkMeterRate(150);

    }

    @Test
    public void testRatesCounters() {
        MetricsBuilder builder = new MetricsBuilder(reporter);
        builder.counter(500).report();
        internal.checkUnreportedMetric(CassandraReporter.rateMetricName(COUNTER));
        builder.counter(750).report();
        internal.checkCounterRate(250);
        builder.counter(750).report();
        internal.checkCounterRate(0);
    }

    @Test
    public void testRatesMeters() {
        MetricsBuilder builder = new MetricsBuilder(reporter);
        builder.meter(500).report();
        internal.checkUnreportedMetric(CassandraReporter.rateMetricName(METER));
        builder.meter(1236).report();
        internal.checkMeterRate(736);
        builder.meter(8000).report();
        internal.checkMeterRate(6764);
    }

    @Test
    public void testRatesTimers() {
        MetricsBuilder builder = new MetricsBuilder(reporter);
        builder.timer(snapshot, 500).report();
        internal.checkUnreportedMetric(CassandraReporter.rateMetricName(TIMER));
        builder.timer(snapshot, 765).report();
        internal.checkTimerRate(265);
        builder.timer(snapshot, 1200).report();
        internal.checkTimerRate(435);
    }

    @Test
    public void testBlackFilters() {
        CassandraReporter reporter = CassandraReporter.forRegistry(registry)
                .reporter(internal)
                .durationUnit(DURATION_UNIT)
                .filter(Arrays.asList(
                        "^.*.75th.*",
                        "^.*.90th.*"
                ), FilterColor.BLACK)
                .build();
        MetricsBuilder builder = new MetricsBuilder(reporter);
        builder.timer(snapshot, 100).report();
        internal.checkUnreportedMetric(TIMER + CassandraReporter._75TH + TIMER_SUFFIX)
                .checkMetric(TIMER + CassandraReporter._99TH + TIMER_SUFFIX, percentiles[3])
                .clear();

        builder.timer(snapshot, 100).report();
        internal.checkUnreportedMetric(TIMER + CassandraReporter._75TH + TIMER_SUFFIX)
                .checkMetric(TIMER + CassandraReporter._99TH + TIMER_SUFFIX, percentiles[3])
                .clear();
    }

    @Test
    public void testWhiteFilters() {
        CassandraReporter reporter = CassandraReporter.forRegistry(registry)
                .reporter(internal)
                .durationUnit(DURATION_UNIT)
                .filter(Arrays.asList(
                        "^.*.75th.*",
                        "^.*.90th.*"
                ), FilterColor.WHITE)
                .build();
        MetricsBuilder builder = new MetricsBuilder(reporter);
        builder.timer(snapshot, 100).report();
        internal.checkMetric(TIMER + CassandraReporter._75TH + TIMER_SUFFIX, percentiles[1])
                .checkUnreportedMetric(TIMER + CassandraReporter._99TH + TIMER_SUFFIX)
                .clear();

        builder.timer(snapshot, 100).report();
        internal.checkMetric(TIMER + CassandraReporter._75TH + TIMER_SUFFIX, percentiles[1])
                .checkUnreportedMetric(TIMER + CassandraReporter._99TH + TIMER_SUFFIX)
                .clear();
    }
}
