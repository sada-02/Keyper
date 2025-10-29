package ru.ifmo.ctddev.igushkin.dkvs

import java.util.HashMap
import java.util.HashSet

/**
 * Represents Replica of Multi-Paxos protocol.
 * Replicas are bridges between Clients and abstraction layer which
 * actually makes consensus decisions.
 *
 * For complete description, see [Paxos Made Moderately Complex]
 * [http://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf]
 *`
 * @param id Replica identifier, unique across the protocol instance.
 * @param send (nodeId, message) Way to send messages to the other nodes.
 * @param leaderIds List of ids which the replica will ask for decisions.
 *
 * @property state Replica state. Paxos supports any abstract state, but
 * in DKVS the state is a key-value string map.
 *
 * @property slotIn Next slot to propose a request to.
 * @property slotOut First slot with non-applied request.
 *
 * @property requests [ClientRequest]s which haven't been proposed yet
 * @property proposals slot -> [ClientRequest] which have been proposed for certain slots but haven''t been processed by [Leader]s yet
 * @property decisions slot -> [ClientRequest] which have been accepted by [Leader]s
 * @property performed [ClientRequest]s which have already been [perform]ed.
 */

public class Replica(val id: Int,
                     val send: (nodeId: Int, Message) -> Unit,
                     val sendToClient: (clientId: Int, text: String) -> Unit,
                     val leaderIds: List<Int>,
                     val persistence: Persistence
) {

    private val state: MutableMap<String, String> = persistence.keyValueStorage!!

    public volatile var slotOut: Int = persistence.lastSlotOut + 1; private set
    public volatile var slotIn: Int = slotOut; private set

    private val awaitingClients = HashMap<OperationDescriptor, Int>()

    private val requests = HashSet<OperationDescriptor>()
    private val proposals = HashMap<Int, OperationDescriptor>()
    private val decisions = HashMap<Int, OperationDescriptor>()

    private val performed = HashSet<OperationDescriptor>()

    private fun perform(c: OperationDescriptor) {
        NodeLogger.logProtocol("PERFORMING $c at $slotOut")
        if (c in performed)
            return
        when (c.request) {
            is SetRequest    -> {
                state[c.request.key] = c.request.value
                val awaitingClient = awaitingClients[c]
                if (awaitingClient != null) {
                    sendToClient(awaitingClient, "STORED")
                    awaitingClients remove awaitingClient
                }
            }
            is DeleteRequest -> {
                val result = (state remove c.request.key) != null
                val awaitingClient = awaitingClients[c]
                if (awaitingClient != null) {
                    sendToClient(awaitingClient, if (result) "DELETED" else "NOT_FOUND")
                    awaitingClients remove awaitingClient
                }
            }
        }
        performed add c
        if (c.request !is GetRequest)
            persistence.saveToDisk("slot $slotOut $c")
    }

    private fun propose() {
        while (requests.isNotEmpty()) {
            val c = requests.first()
            NodeLogger.logProtocol("PROPOSING $c to $slotIn")
            if (slotIn !in decisions) {
                requests remove c
                proposals[slotIn] = c
                leaderIds.forEach { send(it, ProposeMessage(id, slotIn, c)) }
            }
            ++slotIn
        }
    }

    /**
     * Should be called from the replica's container to pass to the replica each message
     * addressed to it.
     * @param message Message that should be handled by the replica.
     */
    public fun receiveMessage(message: ReplicaMessage) {
        when (message) {
            is GetRequest      -> {
                sendToClient(message.fromId,
                             if (message.key in state)
                                 "VALUE ${message.key} ${state[message.key]}" else
                                 "NOT_FOUND")
            }
            is ClientRequest   -> {
                val op = OperationDescriptor(message, id)
                requests add op
                awaitingClients[op] = message.fromId
            }
            is DecisionMessage -> {
                NodeLogger.logProtocol("DECISION $message")
                val slot = message.slot
                decisions.put(slot, message.request)

                while (slotOut in decisions) {
                    val cmd = decisions[slotOut]!!
                    if (slotOut in proposals) {
                        val proposalCmd = proposals[slotOut]
                        proposals remove slotOut
                        if (cmd != proposalCmd) {
                            requests add proposalCmd
                        }
                    }
                    perform(cmd)
                    ++slotOut
                }
            }
        }
        propose()
    }

    /**
     * [Replica]'s container should call [tick] periodically.
     */
    public fun tick() {
        leaderIds.forEach { send(it, SlotOutMessage(id, slotOut)) }
    }
}