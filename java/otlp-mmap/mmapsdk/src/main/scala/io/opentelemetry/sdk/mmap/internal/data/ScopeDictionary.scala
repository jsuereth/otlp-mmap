package io.opentelemetry.sdk.mmap.internal
package data

import io.opentelemetry.api.common.Attributes

class ScopeDictionary(d: Dictionary, strings: StringDictionary):
    private val memos =
        // TODO - don't share just one scope-per-name
        java.util.concurrent.ConcurrentHashMap[String, Long]
    def intern(
        resource_ref: Long, 
        name: String,
        version: String, 
        schema_url: String,
        attributes: Attributes): Long =
            memos.computeIfAbsent(name, name => {
                val data = opentelemetry.proto.mmap.v1.Mmap.InstrumentationScope.newBuilder()
                data.setNameRef(strings.intern(name))
                data.setVersionRef(strings.intern(version))
                data.setResourceRef(resource_ref)
                attributes.forEach((k,v) => {
                    data.addAttributes(AttributeHelper.convertKv(strings)(k,v))
                })
                // TODO - SchemaURL
                d.write(data.build())
            })
