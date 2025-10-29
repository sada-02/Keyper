package ru.ifmo.ctddev.igushkin.dkvs

import java.io.BufferedReader
import java.io.IOException
import java.net.*
import java.util.HashMap
import java.util.concurrent.LinkedBlockingDeque
import kotlin.concurrent.thread
import kotlin.concurrent.timer
import kotlin.properties.Delegates

/**
 * Represents a single independent unit of DKVS/Multi-Paxos.
 * Given 2f+1 [Node]s run simultaneously, in connection with each other,
 * the protocol is tolerant to the fail of 'f' nodes.
 *
 * A node contains one [Replica], one [Leader] and one [Acceptor] in itself.
 *
 * @param id Node identifier which should be unique across the system instance.
 */
public class Node(val id: Int) : Runnable, AutoCloseable {

    private val serverSocket = ServerSocket(GLOBAL_CONFIG.port(id))

    private volatile var started = false
    private volatile var stopping = false

    private val sender = { to: Int, m: Message -> send(to, m); }
    private val clientSender = { to: Int, s: String -> sendToClient(to, s) }
    private val allIds = GLOBAL_CONFIG.ids

    private val persistence = Persistence(id)
    private val localReplica = Replica(id, sender, clientSender, allIds, persistence)
    private val localLeader = Leader(id, sender, allIds, allIds, persistence)
    private val localAcceptor = Acceptor(id, sender, allIds, persistence)

    /**
     * Suspends messages handling for [ms] millis.
     * For tests onlu. Should never be used in production.
     *
     * refactor Inherit Node in tests, do it there.
     */
    public fun sleep(ms: Long) {
        thread {
            mainThread.suspend()
            Thread.sleep(ms)
            mainThread.resume()
        }
    }

    var mainThread: Thread by Delegates.notNull()

    override public fun run() {

        if (started)
            throw IllegalStateException("Cannot start a node which has already been started.")

        started = true

        for (i in 1..GLOBAL_CONFIG.nodesCount)
            if (i != id)
            /** Spawn communication thread. */
                thread { speakToNode(i) }

        mainThread = thread {
            handleMessages()
        }

        monitorFaults()
        pingIfIdle()
        tickReplica()

        thread {
            while (!stopping)
                try {
                    val client = serverSocket.accept()
                    NodeLogger.logConn("Accepted connection from ${client.getRemoteSocketAddress()}.")
                    /** Spawn communication thread. */
                    thread { handleRequest(client) }
                } catch (ignored: SocketException) {
                }
        }

        localLeader.afterRun()
    }

    override fun close() {
        stopping = true
        serverSocket.close()
        for (n in nodes.values() + clients.values()) {
            with(n) {
                input?.close()
                output?.close()
            }
        }
    }

    private data class ConnectionEntry(var input: Socket? = null,
                                       var output: Socket? = null) {

        volatile public var ready: Boolean = false; private set

        public fun setReady() {
            ready = true;
        }

        public fun resetOutput() {
            output?.close()
            output = Socket();
            ready = false;
            messages.retainAll(messages.filter { it !is PingMessage })
        }

        val messages: LinkedBlockingDeque<Message> = LinkedBlockingDeque()
        volatile var aliveIn = false
        volatile var aliveOut = false
    }

    private val nodes = HashMap(GLOBAL_CONFIG.ids.map { it to ConnectionEntry() }.toMap())
    private val clients = sortedMapOf<Int, ConnectionEntry>()

    private fun pingIfIdle() {
        timer(period = GLOBAL_CONFIG.timeout / 4) {
            nodes.entrySet().filter {
                it.key != id &&
                it.value.ready
            }.forEach { p ->
                val (id, n) = p
                if (!n.aliveOut) {
                    send(id, PingMessage())
                }
                n.aliveOut = false
            }
        }
    }

    private fun monitorFaults() {
        val faultyNodes = hashSetOf<Int>()

        timer(period = GLOBAL_CONFIG.timeout) {
            faultyNodes.clear()

            nodes.entrySet().filter { it.key != id } forEach { p ->
                val (i, it) = p
                if (!it.aliveIn) {
                    it.input?.close()
                    faultyNodes.add(i)
                    NodeLogger.logConn("Node $i is faulty, closing its connection.")
                }
                it.aliveIn = false
            }

            if (faultyNodes.size() > 0)
                localLeader.notifyFault(faultyNodes)
        }
    }

    private fun tickReplica() {
        timer(period = GLOBAL_CONFIG.timeout) {
            localReplica.tick()
        }
    }

    private fun sendFirst(to: Int, message: Message) {
        if (to == id)
            eventQueue addFirst message
        else
            nodes[to]!!.messages addFirst  message
    }

    private fun send(to: Int, message: Message) {
        if (to == id)
            eventQueue add message
        else
            nodes[to]!!.messages add message
    }

    private fun sendToClient(to: Int, message: String) {
        clients[to]?.messages?.offer(TextMessage(message))
    }

    /**
     * Executed in new thread, it decides what kind of connection [client] belongs to
     * and switches to [listenToNode] or [listenToClient]
     */
    private fun handleRequest(client: Socket) {
        val reader = client.getInputStream().reader(CHARSET).buffered()
        try {
            val l = reader.readLine()
            val parts = l.split(' ')
            when (parts[0]) {
                "node"                         -> {
                    val nodeId = parts[1].toInt()
                    if (nodeId !in nodes)
                        nodes[nodeId] = ConnectionEntry(client)
                    with (nodes[nodeId]!!) {
                        input?.close()
                        input = client
                    }
                    listenToNode(reader, nodeId)

                }
                "get", "set", "delete", "ping" -> {
                    val newClientId = (clients.keySet().max() ?: 0) + 1
                    clients[newClientId] = ConnectionEntry(client)

                    // Since we've already read a message, we have to handle it on the spot
                    val firstMessage = ClientRequest.parse(newClientId, parts)
                    if (firstMessage != null) {
                        receiveClientRequest(firstMessage)

                        /** Spawn communication thread. */
                        thread { speakToClient(newClientId) }

                        listenToClient(reader, newClientId)
                    }

                }
            }
        } catch (ignored: SocketException) {
        } catch (e: IOException) {
            NodeLogger.logErr("I/O error", e)
        }
    }

    /**
     * Executed in main thread, it takes received messages one by one from
     * [eventQueue] and handles them by forwarding them to the proper receivers.
     */
    private fun handleMessages() {
        while (!stopping) {
            val m = eventQueue.take()
            NodeLogger.logMsgHandle(m)
            if (m is ReplicaMessage)
                localReplica.receiveMessage(m)
            if (m is LeaderMessage)
                localLeader.receiveMessage(m)
            if (m is AcceptorMessage)
                localAcceptor.receiveMessage(m)
        }
    }


    /**
     * Messages from this queue are polled and handled by handleMessages.
     * Every communication thread puts its received messages into the queue.
     */
    val eventQueue = LinkedBlockingDeque<Message>()

    /**
     * Executed in a communication thread, it puts all the messages received from
     * another nodes into [eventQueue].
     */
    private fun listenToNode(reader: BufferedReader, nodeId: Int) {
        NodeLogger.logConn("Started listening to node $nodeId")
        nodes[nodeId]!!.aliveIn = true
        forSplittedLines(reader) { parts ->
            nodes[nodeId]!!.aliveIn = true
            val message = Message.parse(parts)
            NodeLogger.logMsgIn(message, nodeId)

            if (message is PingMessage)
                send(nodeId, PongMessage())
            else
                eventQueue.add(message)

        }
    }

    /**
     * Executed in a communication thread, it puts all the messages received from
     * a client into [eventQueue].
     */
    private fun listenToClient(reader: BufferedReader, clientId: Int) {
        NodeLogger.logConn("Client $clientId connected.")
        try {
            forSplittedLines(reader) { parts ->
                val message = ClientRequest.parse(clientId, parts)
                if (message != null)
                    receiveClientRequest(message)
            }
        } catch (e: SocketException) {
            NodeLogger.logConn("Lost connection to Client $clientId: $e")
        }
    }

    private fun receiveClientRequest(request: ClientRequest) {
        NodeLogger.logMsgIn(request, request.fromId)
        if (request is PingRequest)
            sendToClient(request.fromId, "PONG")
        else
            eventQueue add request
    }


    private fun speakToNode(nodeId: Int) {
        val node = nodes[nodeId]

        val address = GLOBAL_CONFIG.address(nodeId)
        val port = GLOBAL_CONFIG.port(nodeId)

        while (!stopping) {
            try {
                node.resetOutput()
                val socket = node.output!!
                socket.connect(InetSocketAddress(address, port))
                NodeLogger.logConn("Connected to node $nodeId.")
                sendFirst(nodeId, NodeMessage(id))
                val writer = socket.getOutputStream().writer(CHARSET)

                nodes[nodeId].setReady()

                while (true) {
                    nodes[nodeId].aliveOut = true

                    val m = nodes[nodeId].messages.take()
                    try {
                        NodeLogger.logMsgOut(m, nodeId)
                        writer.write("$m\n")
                        writer.flush()
                    } catch (ioe: IOException) {
                        NodeLogger.logErr("Couldn't send $m to $nodeId. Retrying.", ioe)
                        sendFirst(nodeId, m)
                        break
                    }
                }

            } catch (ignored: ConnectException) {
            } catch (e: SocketException) {
                NodeLogger.logErr("Connection to node $nodeId lost.", e)
            }
        }
    }

    private fun speakToClient(clientId: Int) {
        val entry = clients[clientId]!!
        val queue = entry.messages
        val writer = entry.input!!.getOutputStream().writer()
        while (!stopping) {
            val m = queue.take()
            try {
                NodeLogger.logMsgOut(m, clientId)
                writer write "$m\n"
                writer.flush()
            } catch (ioe: IOException) {
                NodeLogger.logErr("Couldn't send a message. Retrying.", ioe)
                nodes[clientId].messages.addFirst(m)
            }
        }
    }
}