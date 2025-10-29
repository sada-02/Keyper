package ru.ifmo.ctddev.igushkin.dkvs.test

import ru.ifmo.ctddev.igushkin.dkvs.CHARSET
import java.io.BufferedReader
import java.io.OutputStreamWriter
import java.net.InetSocketAddress
import java.net.Socket
import java.net.SocketException
import kotlin.properties.Delegates

/**
 * Simple wrapper which connects to DKVS node and performs requests.
 *
 * Created by Sergey on 25.05.2015.
 */

val SOCKET_TIMEOUT = 10000

public open class Client(val address: String, val port: Int) {

    private var socket: Socket by Delegates.notNull()
    private var reader: BufferedReader by Delegates.notNull()
    private var writer: OutputStreamWriter by Delegates.notNull()

    private val endpoint = InetSocketAddress(address, port)

    public open synchronized fun connect() {
        socket = Socket()
        socket.setSoTimeout(SOCKET_TIMEOUT)
        socket.connect(endpoint)
        reader = socket.getInputStream().reader(CHARSET).buffered()
        writer = socket.getOutputStream().writer(CHARSET)
    }

    public open synchronized fun disconnect() {
        socket.close()
        socket = Socket()
    }

    public open synchronized fun get(key: String): String? {
        try {
            writer.write("get $key\n")
            writer.flush()
            val responseParts = reader.readLine().split(' ')
            return when (responseParts[0]) {
                "NOT_FOUND" -> null
                "VALUE"     -> responseParts.subList(2, responseParts.size()).join(" ")
                else        -> throw RuntimeException("Incorrect response: ${responseParts.join(" ")}");
            }
        } catch (e: SocketException) {
            return null
        }
    }

    public open synchronized fun set(key: String, value: String): Boolean {
        try {
            writer.write("set $key $value\n")
            writer.flush()
            val response = reader.readLine()
            return response == "STORED"
        } catch (e: SocketException) {
            return false
        }
    }

    public open synchronized fun delete(key: String, value: String): Boolean {
        try {
            writer.write("delete $key\n")
            writer.flush()
            val response = reader.readLine()
            return response == "DELETED"
        } catch (e: SocketException) {
            return false
        }
    }
}