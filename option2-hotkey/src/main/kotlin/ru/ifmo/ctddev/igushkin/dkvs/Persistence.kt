package ru.ifmo.ctddev.igushkin.dkvs

import java.io.File
import java.io.FileWriter
import java.util.ArrayList
import java.util.HashMap

/**
 * Holds the disk storage for node.
 *
 * Created by Sergey on 24.05.2015.
 */

public class Persistence(val nodeId: Int) {

    public val fileName: String = "dkvs_$nodeId.log"
    private val writer = FileWriter(fileName, true).buffered()

    public volatile var lastBallotNum: Int = 0; private set
    public volatile var keyValueStorage: HashMap<String, String>? = null

    public volatile var lastSlotOut: Int = 0;

    public fun nextBallotNum(): Int {
        return ++lastBallotNum
    }

    init {
        val file = File(fileName)
        if (file.exists()) {
            val reader = file.reader().buffered()
            val lines = ArrayList<String>()
            for (l in reader.lines())
                lines add l

            val storage = hashMapOf<String, String>()
            val removedKeys = hashSetOf<String>()

            loop@ for (l in lines.reverse()) {
                val parts = l.split(' ')
                when (parts[0]) {
                    "ballot" -> lastBallotNum = Math.max(lastBallotNum, Ballot.parse(parts[1]).ballotNum)
                    "slot" -> {
                        val key = if (4 in parts.indices) parts[4] else null
                        lastSlotOut = Math.max(lastSlotOut, parts[1].toInt())
                        if (key in storage || key in removedKeys)
                            continue@loop
                        when (parts[3]) {
                            "set" -> storage[key] = parts[5..parts.lastIndex].join(" ")
                            "delete" -> removedKeys add key
                        }
                    }
                }
            }

            keyValueStorage = storage

            for (l in lines.reverse()) {
                val parts = l.split(' ')
                if (parts[0] == "ballot") {
                    lastBallotNum = Ballot.parse(parts[1]).ballotNum
                    break
                }
            }
        }
    }

    public fun saveToDisk(s: String) {
        with(writer) {
            write(s)
            newLine()
            flush()
        }
    }
}