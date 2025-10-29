package ru.ifmo.ctddev.igushkin.dkvs.meta

/**
 * Created by Sergey on 17.05.2015.
 */

fun main(args: Array<String>) {
    val arg =
            if (0 in args.indices)
                args[0] else
                readLine()!!
    println(variant(arg))
}

fun variant(lastName: String): Int =
        ((lastName.toUpperCase()).hashCode() and 0x7fffffff) % 3 + 1