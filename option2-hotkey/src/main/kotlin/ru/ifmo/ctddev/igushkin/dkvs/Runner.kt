package ru.ifmo.ctddev.igushkin.dkvs

import kotlin.platform.platformStatic

/**
 * Created by Sergey on 23.05.2015.
 *
 * Runs dkvs [Node]s listed in [ids].
 */

class Runner(val ids: List<Int>) {
    companion object {
        platformStatic public fun main(args: Array<String>) {
            val runner = Runner(if (0 in args.indices)
                                    listOf(args[0].toInt()) else
                                    GLOBAL_CONFIG.ids)
            runner.runAll()
        }
    }

    val nodes = ids map { Node(it) }

    fun run(id: Int) {
        nodes[id].run()
    }

    fun runAll() {
        nodes forEach { it.run() }
    }

    fun closeAll() {
        nodes forEach { it.close() }
    }

}