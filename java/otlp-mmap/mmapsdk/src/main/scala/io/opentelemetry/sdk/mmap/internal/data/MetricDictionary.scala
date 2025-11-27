package io.opentelemetry.sdk.mmap.internal
package data

import io.opentelemetry.sdk.mmap.internal.Dictionary

class MetricDictionary(d: Dictionary, strings: StringDictionary):
    // Metric name to index lookup
    private val memos = java.util.concurrent.ConcurrentHashMap[String, Long]

    def intern(metric: opentelemetry.proto.mmap.v1.Mmap.MetricRef): Long =
        memos.computeIfAbsent(metric.getName(), _ => d.write(metric))
