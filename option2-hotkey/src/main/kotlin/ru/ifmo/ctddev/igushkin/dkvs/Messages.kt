package ru.ifmo.ctddev.igushkin.dkvs

import java.util.LinkedHashSet

/**
 * Messages between [Node]s and Clients.
 *
 * Overriding of toString is used to serialize messages into
 * string representation and send them.
 */
public interface Message {
    companion object {
        public fun parse(s: String): Message = parse(s.split(' '))

        public fun parse(parts: List<String>): Message {
            val p = parts.lastIndex;
            return when (parts[0]) {
                "node"     -> NodeMessage(parts[1].toInt())
                "ping"     -> PingMessage()
                "pong"     -> PongMessage()
                "decision" -> DecisionMessage(parts[1].toInt(), OperationDescriptor.parse(parts[2..p]))
                "propose"  -> ProposeMessage(parts[1].toInt(), parts[2].toInt(), OperationDescriptor.parse(parts[3..p]))
                "p1a"      -> PhaseOneRequest(parts[1].toInt(), Ballot.parse(parts[2]))
                "p2a"      -> PhaseTwoRequest(parts[1].toInt(), AcceptProposal.parse(parts[2..p]))
                "p1b"      -> PhaseOneResponse.parse(parts)
                "p2b"      -> PhaseTwoResponse(parts[1].toInt(), Ballot.parse(parts[2]), AcceptProposal.parse(parts[3..p]))
                "slotOut"  -> SlotOutMessage(parts[1].toInt(), parts[2].toInt())
                else       -> throw IllegalArgumentException("Unknown message.")
                //for "get", "set", "delete" use ClientRequest.parse(...)
            }
        }
    }
}

//region Node messages

/**
 * Message which is sent first in order to establish connection between [Node]s.
 */
public class NodeMessage(val fromId: Int) : Message {
    override fun toString() = "node $fromId"
}

/**
 * Request for checking connectivity between [Node]s.
 * [PongMessage] is the right response.
 */
public class PingMessage() : Message {
    override fun toString() = "ping"
}

/**
 * Response for [PingMessage] which shows positive connectivity.
 */
public class PongMessage() : Message {
    override fun toString() = "pong"
}

//endregion Node messages

//region Client requests

/**
 * Sub-hierarchy of messages addressed to [Replica]s.
 */
public abstract class ReplicaMessage(val fromId: Int) : Message

public class DecisionMessage(val slot: Int, val request: OperationDescriptor) : ReplicaMessage(-1) {
    override fun toString() = "decision $slot $request"
}

/**
 * Sub-hierarchy of client requests.
 * These represent certain application (DKVS) and not Paxos itself.
 *
 * Client messages are only received and dispatched to replicas and are never sent
 * themselves but can still be sent as payload.
 *
 * @param clientId Node-local client id.
 */
public abstract class ClientRequest(clientId: Int) : ReplicaMessage(clientId) {

    companion object {
        public fun parse(clientId: Int, parts: List<String>): ClientRequest? =
                when (parts[0]) {
                    "get"    -> GetRequest(clientId, parts[1])
                    "set"    -> SetRequest(clientId, parts[1], parts.drop(2).join(" "))
                    "delete" -> DeleteRequest(clientId, parts[1])
                    "ping"   -> PingRequest(clientId)
                    else     -> { NodeLogger.logErr("Invalid client request: ${parts.join(" ")}"); null }
                }
    }
}

public data class GetRequest(fromId: Int, val key: String) : ClientRequest(fromId) {
    override fun toString() = "get $key"
}

public data class SetRequest(fromId: Int, val key: String, val value: String) : ClientRequest(fromId) {
    override fun toString() = "set $key $value"
}

public data class DeleteRequest(fromId: Int, val key: String) : ClientRequest(fromId) {
    override fun toString() = "delete $key"
}

public data class PingRequest(fromId: Int) : ClientRequest(fromId)

//endregion Client requests

//region Leader messages
/**
 * Sub-hierarchy of messages addressed to [Leader]s.
 */
public interface LeaderMessage : Message

/**
 * Sent by [Replica] and contains a proposition of [request] to [slot].
 */
public data class ProposeMessage(val fromId: Int, val slot: Int, val request: OperationDescriptor) : LeaderMessage {
    override fun toString() = "propose $fromId $slot $request"
}

public val payloadSplitter: String = " ### "

/**
 * Sent to [Leader.Scout] from [Acceptor] in response to [PhaseOneRequest].
 */
public class PhaseOneResponse(val fromId: Int,
                              val originalBallot: Ballot,
                              val ballotNum: Ballot,
                              val pvalues: Collection<AcceptProposal>): LeaderMessage {
    override fun toString() = "p1b $fromId $originalBallot $ballotNum ${pvalues.joinToString(payloadSplitter)}"

    companion object {
        fun parse(parts: List<String>): PhaseOneResponse {
            if (parts[0] != "p1b") throw IllegalArgumentException("PhaseOneResponse should start by \"p1b\"")
            val fromId = parts[1].toInt()
            val originalBallot = Ballot.parse(parts[2])
            val ballotNum = Ballot.parse(parts[3])
            val pvalues = parts[4..parts.lastIndex].join(" ").splitBy("$payloadSplitter")
                    .filter { it.length() > 0 }
                    .map { it.split(' ') }
                    .map { AcceptProposal.parse(it) }
            return PhaseOneResponse(fromId, originalBallot, ballotNum, LinkedHashSet(pvalues))
        }
    }
}

/**
 * Sent to [Leader] from [Acceptor] in response to [PhaseTwoRequest].
 */
public class PhaseTwoResponse(val fromId: Int, val ballot: Ballot, val proposal: AcceptProposal) : LeaderMessage {
    override fun toString() = "p2b $fromId $ballot $proposal"
}

//endregion Leader messages

//region Acceptor messages
/**
 * Sub-hierarchy of messages sent to [Acceptor]s.
 */
public interface AcceptorMessage : Message

/**
 * Sent by [Leader.Scout] to [Acceptor].
 * Normal response is [PhaseOneResponse].
 */
public class PhaseOneRequest(val fromId: Int, val ballotNum: Ballot) : AcceptorMessage {
    override fun toString() = "p1a $fromId $ballotNum"
}

/**
 * Sent by active [Leader] to [Acceptor].
 * Normal response is [PhaseTwoResponse].
 */
public class PhaseTwoRequest(val fromId: Int, val payload: AcceptProposal) : AcceptorMessage {
    override fun toString() = "p2a $fromId $payload"
}
//endregion Acceptor messages

//region Garbage collection

/**
 * Sent by [Replica]s to inform [Leader]s and [Acceptor]s of its [Replica.slotOut],
 * which is used for garbage collection.
 */
public class SlotOutMessage(val fromId: Int, val slotOut: Int): LeaderMessage, AcceptorMessage {
    override fun toString() = "slotOut $fromId $slotOut"
}

//endregion Garbage collection

/**
 * Never received by [Node]s.
 * The only usage is for sending responses to the Clients.
 */
public class TextMessage(val text: String) : Message {
    override fun toString() = text
}

