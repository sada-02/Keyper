package ru.ifmo.ctddev.igushkin.dkvs

import java.util.logging.Level
import java.util.logging.Logger

/**
 * Provides support for logging events specific for [Node]s
 *
 * Created by Sergey on 24.05.2015.
 */

public open class NodeLogger() {
    private val logger = Logger.getLogger("Node")

    private fun Message.isLoggable() =
            this !is PingMessage &&
            this !is PongMessage &&
            this !is SlotOutMessage

    public fun logMsgIn(m: Message, fromId: Int) {
        if (m.isLoggable())
            logger.log(Level.INFO, ">> from $fromId: $m")
    }

    public fun logMsgOut(m: Message, toId: Int) {
        if (m.isLoggable())
            logger.log(Level.INFO, "<< to $toId: $m")
    }

    public fun logMsgHandle(m: Message) {
        if (m.isLoggable())
            logger.log(Level.INFO, ".. handling: $m")
    }

    public fun logProtocol(s: String) {
        logger.log(Level.INFO, "## $s")
    }

    public fun logConn(s: String) {
        logger.log(Level.INFO, "   $s")
    }

    public fun logErr(s: String, t: Throwable? = null) {
        logger.log(Level.SEVERE, "!! $s", t)
    }

    companion object : NodeLogger() {}
}