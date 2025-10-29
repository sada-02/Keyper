package ru.ifmo.ctddev.igushkin.dkvs

import java.io.BufferedReader

/**
 * Useful utils and extensions.
 */

fun forSplittedLines(reader: BufferedReader, f: (List<String>) -> Unit) {
    var line: String? = null
    while ({line = reader.readLine(); line != null}()) {
        f(line!!.split(' '))
    }
}

fun <T> List<T>.get(range: IntRange) = this.subList(range.start, range.end + 1)