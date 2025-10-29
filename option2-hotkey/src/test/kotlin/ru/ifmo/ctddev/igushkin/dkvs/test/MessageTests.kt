package ru.ifmo.ctddev.igushkin.dkvs.test

/**
 * Tests for messages toString & parsing.
 *
 * Created by Sergey on 22.05.2015.
 */

import org.junit.Assert.assertEquals
import ru.ifmo.ctddev.igushkin.dkvs.Message
import ru.ifmo.ctddev.igushkin.dkvs.payloadSplitter
import org.junit.Test as test

val x = 'y';

public class MessageTests {

    private fun checkParseAndToString(message: String) {
        assertEquals(message, Message.parse(message).toString())
    }

    test fun testParsing() {
        checkParseAndToString("node 5")
        checkParseAndToString("ping")
        checkParseAndToString("pong")
        checkParseAndToString("decision 5 <3> get abcAbc")
        checkParseAndToString("propose 12 34 <3> set aaa aaaaa  aa  aa")
        checkParseAndToString("p1a 4 5_555")
        checkParseAndToString("p2a 3 4_123 5 <3333> delete abccc")
        checkParseAndToString("p1b 345 333_15 23_4 1_2 2 <1> get a!!b${payloadSplitter}3_123 4 <5> set a b c${payloadSplitter}5_555 6 <33> delete a")
        checkParseAndToString("p2b 1 3_333 1_1 1 <24> get a")
    }

}