/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchShardTask;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.logging.SlowLogLevel;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.index.shard.SearchOperationListener;
import org.opensearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * The warm search time slow log implementation
 *
 * @opensearch.internal
 */
public final class WarmSearchSlowLog implements SearchOperationListener {
    private long queryWarnThreshold;
    private long queryInfoThreshold;
    private long queryDebugThreshold;
    private long queryTraceThreshold;

    private long fetchWarnThreshold;
    private long fetchInfoThreshold;
    private long fetchDebugThreshold;
    private long fetchTraceThreshold;

    private SlowLogLevel level;

    private final Logger queryLogger;
    private final Logger fetchLogger;

    private static final String WARM_SEARCH_SLOWLOG_PREFIX = "index.warm.slowlog";
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING =
        Setting.timeSetting(WARM_SEARCH_SLOWLOG_PREFIX + ".threshold.query.warn", TimeValue.timeValueMillis(10000),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING =
        Setting.timeSetting(WARM_SEARCH_SLOWLOG_PREFIX + ".threshold.query.info", TimeValue.timeValueMillis(5000),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING =
        Setting.timeSetting(WARM_SEARCH_SLOWLOG_PREFIX + ".threshold.query.debug", TimeValue.timeValueMillis(2000),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING =
        Setting.timeSetting(WARM_SEARCH_SLOWLOG_PREFIX + ".threshold.query.trace", TimeValue.timeValueMillis(500),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING =
        Setting.timeSetting(WARM_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.warn", TimeValue.timeValueMillis(1000),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING =
        Setting.timeSetting(WARM_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.info", TimeValue.timeValueMillis(800),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING =
        Setting.timeSetting(WARM_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.debug", TimeValue.timeValueMillis(500),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<TimeValue> INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING =
        Setting.timeSetting(WARM_SEARCH_SLOWLOG_PREFIX + ".threshold.fetch.trace", TimeValue.timeValueMillis(200),
            TimeValue.timeValueMillis(-1), Property.Dynamic, Property.IndexScope);
    public static final Setting<SlowLogLevel> INDEX_SEARCH_SLOWLOG_LEVEL =
        new Setting<>(WARM_SEARCH_SLOWLOG_PREFIX + ".level", SlowLogLevel.TRACE.name(), SlowLogLevel::parse, Property.Dynamic,
            Property.IndexScope);

    private static final ToXContent.Params FORMAT_PARAMS = new ToXContent.MapParams(Collections.singletonMap("pretty", "false"));

    public WarmSearchSlowLog(IndexSettings indexSettings) {

        this.queryLogger = LogManager.getLogger(WARM_SEARCH_SLOWLOG_PREFIX + ".query");
        this.fetchLogger = LogManager.getLogger(WARM_SEARCH_SLOWLOG_PREFIX + ".fetch");

        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING, this::setQueryWarnThreshold);
        setQueryWarnThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_WARN_SETTING));
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING, this::setQueryInfoThreshold);
        setQueryInfoThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_INFO_SETTING));
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING, this::setQueryDebugThreshold);
        setQueryDebugThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_DEBUG_SETTING));
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING, this::setQueryTraceThreshold);
        setQueryTraceThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_QUERY_TRACE_SETTING));

        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING, this::setFetchWarnThreshold);
        setFetchWarnThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_WARN_SETTING));
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING, this::setFetchInfoThreshold);
        setFetchInfoThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_INFO_SETTING));
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING, this::setFetchDebugThreshold);
        setFetchDebugThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_DEBUG_SETTING));
        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING, this::setFetchTraceThreshold);
        setFetchTraceThreshold(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_THRESHOLD_FETCH_TRACE_SETTING));

        indexSettings.getScopedSettings().addSettingsUpdateConsumer(INDEX_SEARCH_SLOWLOG_LEVEL, this::setLevel);
        setLevel(indexSettings.getValue(INDEX_SEARCH_SLOWLOG_LEVEL));
    }

    private void setLevel(SlowLogLevel level) {
        this.level = level;
        Loggers.setLevel(queryLogger, level.name());
        Loggers.setLevel(fetchLogger, level.name());
    }

    private WarmPerQueryMetric removeMetricCollector() {
        return WarmQueryMetricService.getInstance().removeMetricCollector(Thread.currentThread().getId());
    }

    private Set<WarmPerQueryMetric> removeMetricCollectors(String parentTaskId, String shardId, boolean isQueryPhase) {
        return WarmQueryMetricService.getInstance().removeMetricCollectors(parentTaskId, shardId, isQueryPhase);
    }

    private void setMetricCollector(SearchContext searchContext, boolean isQueryPhase) {
        final SearchShardTask searchTask = searchContext.getTask();
        if (searchTask == null) {
            final Logger logger = isQueryPhase ? queryLogger : fetchLogger;
            logger.error("Warm Slow Log: Search Task not expected to be null");
        }
        WarmQueryMetricService.getInstance().addMetricCollector(Thread.currentThread().getId(),
            new WarmPerQueryMetricImpl(
                searchTask == null ? null : searchTask.getParentTaskId().toString(),
                searchContext.shardTarget().getShardId().toString()
            ),
            isQueryPhase
        );
    }

    @Override
    public void onPreQueryPhase(SearchContext searchContext) {
        // The same search thread can pick up multiple slice executions post https://github.com/apache/lucene/pull/13472
        // so we initialize collectors only in onPreSliceExecution
    }

    @Override
    public void onFailedQueryPhase(SearchContext searchContext) {
        removeMetricCollector();
        removeMetricCollectors(
            searchContext.getTask().getParentTaskId().toString(),
            searchContext.shardTarget().getShardId().toString(),
           true
        );
    }

    @Override
    public void onQueryPhase(SearchContext context, long tookInNanos) {
        // Get all collectors associated with the task/shard
        final List<WarmPerQueryMetric> metricCollectors = new ArrayList<>(removeMetricCollectors(
            context.getTask().getParentTaskId().toString(),
            context.shardTarget().getShardId().toString(),
            true
        ));

        // No need to call removeMetricCollector() here as that will be handled in onSliceExecution in both
        // concurrent search and non-concurrent search cases

        if (queryWarnThreshold >= 0 && tookInNanos > queryWarnThreshold) {
            queryLogger.warn("{}", new WarmSlowLogPrinter(context, tookInNanos, metricCollectors));
        } else if (queryInfoThreshold >= 0 && tookInNanos > queryInfoThreshold) {
            queryLogger.info("{}", new WarmSlowLogPrinter(context, tookInNanos, metricCollectors));
        } else if (queryDebugThreshold >= 0 && tookInNanos > queryDebugThreshold) {
            queryLogger.debug("{}", new WarmSlowLogPrinter(context, tookInNanos, metricCollectors));
        } else if (queryTraceThreshold >= 0 && tookInNanos > queryTraceThreshold) {
            queryLogger.trace("{}", new WarmSlowLogPrinter(context, tookInNanos, metricCollectors));
        }
    }

    @Override
    public void onPreSliceExecution(SearchContext searchContext) {
        setMetricCollector(searchContext, true);
    }

    @Override
    public void onFailedSliceExecution(SearchContext searchContext) {
        removeMetricCollector();
    }

    @Override
    public void onSliceExecution(SearchContext searchContext) {
        removeMetricCollector();
    }

    public void onPreFetchPhase(SearchContext searchContext) {
        // Fetch phase execution is starting. Add new metric collector
        setMetricCollector(searchContext, false);
    }

    @Override
    public void onFailedFetchPhase(SearchContext searchContext) {
        removeMetricCollector();
        removeMetricCollectors(
            searchContext.getTask().getParentTaskId().toString(),
            searchContext.shardTarget().getShardId().toString(),
            false
        );
    }

    @Override
    public void onFetchPhase(SearchContext context, long tookInNanos) {
        removeMetricCollector();
        // Although fetch phase is single threaded today, we will use the same map implementation for posterity.
        // It's also much cleaner than propagating the fetch boolean to WarmQueryMetricService
        final List<WarmPerQueryMetric> metricCollectors = new ArrayList<>(removeMetricCollectors(
            context.getTask().getParentTaskId().toString(),
            context.shardTarget().getShardId().toString(),
            false
        ));
        assert metricCollectors.size() == 1 : "Fetch phase is expected to be single threaded, so we should only have 1 collector" ;
        if (fetchWarnThreshold >= 0 && tookInNanos > fetchWarnThreshold) {
            fetchLogger.warn("{}", new WarmSlowLogPrinter(context, tookInNanos, metricCollectors));
        } else if (fetchInfoThreshold >= 0 && tookInNanos > fetchInfoThreshold) {
            fetchLogger.info("{}", new WarmSlowLogPrinter(context, tookInNanos, metricCollectors));
        } else if (fetchDebugThreshold >= 0 && tookInNanos > fetchDebugThreshold) {
            fetchLogger.debug("{}", new WarmSlowLogPrinter(context, tookInNanos, metricCollectors));
        } else if (fetchTraceThreshold >= 0 && tookInNanos > fetchTraceThreshold) {
            fetchLogger.trace("{}", new WarmSlowLogPrinter(context, tookInNanos, metricCollectors));
        }
    }

    static final class WarmSlowLogPrinter {
        private final SearchContext context;
        private final long tookInNanos;
        private final List<WarmPerQueryMetric> metricCollectors;

        WarmSlowLogPrinter(SearchContext context, long tookInNanos, List<WarmPerQueryMetric> metricCollectors) {
            this.context = context;
            this.tookInNanos = tookInNanos;
            this.metricCollectors = metricCollectors;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("warm_stats[");
            Strings.collectionToDelimitedString(metricCollectors, ",", "", "", sb);
            sb.append("], ");
            sb.append("took[").append(TimeValue.timeValueNanos(tookInNanos)).append("], took_millis[").append(TimeUnit.NANOSECONDS.toMillis(tookInNanos)).append("], ");
            if (context.groupStats() == null) {
                sb.append("stats[], ");
            } else {
                sb.append("stats[");
                Strings.collectionToDelimitedString(metricCollectors, ",", "", "", sb);
                sb.append("]");
                Strings.collectionToDelimitedString(context.groupStats(), ",", "", "", sb);
                sb.append("], ");
            }
            sb.append("search_type[").append(context.searchType()).
                append("], total_shards[").append(context.numberOfShards()).append("], ");
            if (context.request().source() != null) {
                sb.append("source[").append(context.request().source().toString(FORMAT_PARAMS)).append("], ");
            } else {
                sb.append("source[], ");
            }
            return sb.toString();
        }
    }

    private void setQueryWarnThreshold(TimeValue warnThreshold) {
        this.queryWarnThreshold = warnThreshold.nanos();
    }

    private void setQueryInfoThreshold(TimeValue infoThreshold) {
        this.queryInfoThreshold = infoThreshold.nanos();
    }

    private void setQueryDebugThreshold(TimeValue debugThreshold) {
        this.queryDebugThreshold = debugThreshold.nanos();
    }

    private void setQueryTraceThreshold(TimeValue traceThreshold) {
        this.queryTraceThreshold = traceThreshold.nanos();
    }

    private void setFetchWarnThreshold(TimeValue warnThreshold) {
        this.fetchWarnThreshold = warnThreshold.nanos();
    }

    private void setFetchInfoThreshold(TimeValue infoThreshold) {
        this.fetchInfoThreshold = infoThreshold.nanos();
    }

    private void setFetchDebugThreshold(TimeValue debugThreshold) {
        this.fetchDebugThreshold = debugThreshold.nanos();
    }

    private void setFetchTraceThreshold(TimeValue traceThreshold) {
        this.fetchTraceThreshold = traceThreshold.nanos();
    }

    long getQueryWarnThreshold() {
        return queryWarnThreshold;
    }

    long getQueryInfoThreshold() {
        return queryInfoThreshold;
    }

    long getQueryDebugThreshold() {
        return queryDebugThreshold;
    }

    long getQueryTraceThreshold() {
        return queryTraceThreshold;
    }

    long getFetchWarnThreshold() {
        return fetchWarnThreshold;
    }

    long getFetchInfoThreshold() {
        return fetchInfoThreshold;
    }

    long getFetchDebugThreshold() {
        return fetchDebugThreshold;
    }

    long getFetchTraceThreshold() {
        return fetchTraceThreshold;
    }

    SlowLogLevel getLevel() {
        return level;
    }
}

