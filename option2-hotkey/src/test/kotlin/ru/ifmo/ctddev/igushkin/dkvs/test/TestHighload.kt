package ru.ifmo.ctddev.igushkin.dkvs.test

/**
 * Tests which load DKVS at maximum rate.
 *
 * Created by Sergey on 25.05.2015.
 */

import org.junit.Assert.assertTrue
import ru.ifmo.ctddev.igushkin.dkvs.Configuration
import ru.ifmo.ctddev.igushkin.dkvs.Runner
import java.io.File
import kotlin.concurrent.thread
import org.junit.After as after
import org.junit.Before as before
import org.junit.Test as test

public class TestHighload {

    fun cleanLogs(c: Configuration) {
        for (i in c.ids) {
            File("dkvs_$i.log").delete()
        }
    }

    val config = Configuration.readDkvsProperties()

    val runner = Runner(config.ids)

    public before fun setUp() {
        cleanLogs(config)
        runner.runAll()
    }

    public after fun tearDown() {
        runner.closeAll()
    }

    val keys = 10;
    val iterationsPerKey = 20;

    private fun runClient(nodeId: Int, keys: Iterable<Int>, clientFactory: (String, Int) -> Client) {
        val client = clientFactory(config.address(nodeId), config.port(nodeId))
        client.connect()

        for (key in keys) {
            for (v in 1..iterationsPerKey) {
                if (client.set("$key", "$v")) {
                    val storedValue = client["$key"]
                    assertTrue(storedValue?.equals("$v") ?: true)
                }
            }
        }
    }

    test fun singleClient() {
        thread {
            runClient(1, 1..keys, { a, p -> Client(a, p) })
        }.join()

    }

    test fun multipleClients() {
        val threads = Array(config.ids.size(),
                            { thread { runClient(it, keys * it..keys * (it + 1) - 1, { a, p -> Client(a, p) }) } });

        threads.forEach { it.join() }
    }

    inner class ClientWithSleep(address: String, port: Int) : Client(address, port) {

        val id = config.ids.firstOrNull { config.address(it) == address && config.port(it) == port }!!

        fun sleep(ms: Long) {
            runner.nodes[id].sleep(ms)
        }

        val sleepFrequency = 20
        var nextSleepIn = sleepFrequency
        val defaultSleepTime = 3000L

        override fun get(key: String): String? {
            val result = super.get(key)
            nextSleepIn--
            if (nextSleepIn == 0) {
                nextSleepIn = sleepFrequency
                sleep(defaultSleepTime)
            }
            return result
        }
    }

    test fun withSleep() {
        thread {
            runClient(1, 1..keys, { a, p -> ClientWithSleep(a, p) })
        }.join()
    }

    test fun multipleClientsHalfWithSleep() {
        val threads = Array(config.ids.size(),
                            {
                                thread {
                                    runClient(it,
                                              keys * it..keys * (it + 1) - 1,
                                              { a, p -> ClientWithSleep(a, p) })
                                }
                            });

        threads.forEach { it.join() }
    }


}