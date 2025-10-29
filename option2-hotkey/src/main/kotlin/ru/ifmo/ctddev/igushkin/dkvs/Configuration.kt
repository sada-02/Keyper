package ru.ifmo.ctddev.igushkin.dkvs

import java.util.Collections
import java.util.Properties

/**
 * Stores nodes configuration which consists of:
 * @property addresses mapping of node ids to network addresses on which the nodes can be found
 * @property timeout idleness limit for nodes communication
 */

public data class Configuration(val addresses: Map<Int, String>,
                                val timeout: Long
) {
    public fun port(id: Int): Int {
        if (id !in addresses)
            throw IllegalArgumentException("ID out of configuration.")
        val parts = addresses[id]!!.splitBy(":")
        return parts[1].toInt()
    }

    public fun address(id: Int): String {
        if (id !in addresses)
            throw IllegalArgumentException("ID out of configuration.")
        val parts = addresses[id]!!.splitBy(":")
        return parts[0]
    }

    public val nodesCount: Int
        get() = addresses.size()

    public val ids: List<Int>
        get() = (1..nodesCount).toList()

    companion object {
        public fun readDkvsProperties(filename: String = "dkvs.properties"): Configuration {

            val NODE_ADDRESS_PREFIX = "node"

            val input = javaClass<Configuration>().getClassLoader().getResourceAsStream(filename)
            val props = Properties()
            props.load(input)

            val timeout = (props["timeout"] as String).toLong()
            val addresses = hashMapOf<Int, String>()
            for ((k, v) in props.entrySet()) {
                if (k !is String || v !is String)
                    continue
                if (k.startsWith(NODE_ADDRESS_PREFIX)) {
                    val parts = k.splitBy(".")
                    val id = parts[1].toInt()
                    addresses[id] = v
                }
            }

            return Configuration(Collections.unmodifiableMap(addresses), timeout)
        }
    }
}

public val GLOBAL_CONFIG: Configuration = Configuration.readDkvsProperties();

public val CHARSET: String = "UTF-8"