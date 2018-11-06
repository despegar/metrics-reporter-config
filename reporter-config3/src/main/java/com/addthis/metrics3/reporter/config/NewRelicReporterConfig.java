package com.addthis.metrics3.reporter.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.addthis.metrics.reporter.config.AbstractMetricReporterConfig;
import com.addthis.metrics3.reporter.CassandraReporter;
import com.addthis.metrics3.reporter.Converter;
import com.addthis.metrics3.reporter.SimpleReporter;
import com.addthis.metrics3.reporter.UnitConvertion;
import com.codahale.metrics.MetricRegistry;
import com.newrelic.api.agent.NewRelic;


public class NewRelicReporterConfig extends AbstractMetricReporterConfig implements MetricsReporterConfigThree {

    private List<ConvertionConfig> convertions;

    private static final Logger logger = LoggerFactory.getLogger(NewRelicReporterConfig.class);

    protected String prefix;

    protected boolean enabled = false;

    @Override
    public boolean enable(MetricRegistry registry) {
        if (isEnabled()) {
            logger.info("Enabling NewRelicReporter to {}", getPrefix());
            try {
                // static enable() methods omit the option of specifying a
                // predicate.  Calling constructor and starting manually
                // instead
                CassandraReporter reporter = CassandraReporter.forRegistry(registry)
                        .name("new relic reporter")
                        .filter(MetricFilterTransformer.generateFilter(getPredicate()))
                        .rateUnit(TimeUnit.SECONDS)
                        .reporter(new SimpleReporter() {

                            final String prefix = "Custom/" + getPrefix() + "/";

                            Converter converter = new Converter(getCompiledConversions());

                            @Override
                            public void report(String name, float value) {
                                String finalName = prefix + name.split("org.apache.cassandra.metrics.")[1].replace(".", "/");
                                float finalValue = converter.convert(name, value);
                                logger.trace("Reporting metric {} with value {}", finalName, finalValue);
                                NewRelic.recordMetric(finalName, finalValue);
                            }
                        })
                        .durationUnit(TimeUnit.MILLISECONDS)
                        .build();

                reporter.start(getPeriod(), getRealTimeunit());
            } catch (Exception e) {
                logger.error("Failure while Enabling NewRelicReporter", e);
                return false;
            }
            return true;
        }
        else {
            logger.warn("NewRelicReporter is disabled");
            return false;
        }
    }

    @Override
    public void report() {
    }

    private Map<Pattern, UnitConvertion> getCompiledConversions(){
        Map<Pattern, UnitConvertion> result = new HashMap<Pattern, UnitConvertion>();
        for (ConvertionConfig c : convertions){
            try {
                result.put(Pattern.compile(c.getPattern()), UnitConvertion.fromString(c.getConvertion()));
            }
            catch (Exception e) {
                logger.error("UnitConversion {} wasn't found", c);
            }
        }
        return result;
    }

    public String getPrefix() {
        return prefix;
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public List<ConvertionConfig> getConvertions(){
        if (convertions == null)
            return Collections.emptyList();
        return convertions;
    }

    public void setConvertions(List<ConvertionConfig> convertions) {
        this.convertions = convertions;
    }
}
