package ru.ifmo.ctddev.igushkin.dkvs

import java.util.HashMap

/**
 * Represents [Acceptor] part of Multi-Paxos protocol.
 * Acceptors contain fault-tolerant memory of the protocol and
 * actually decide on current [Leader] and the proposals accepted.
 *
 * For complete description, see [Paxos Made Moderately Complex]
 * [http://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf]
 */

public class Acceptor(val id: Int,
                      val send: (leaderId: Int, Message) -> Unit,
                      val replicaIds: List<Int>,
                      val persistence: Persistence

) {
    private volatile var ballotNumber = Ballot(persistence.lastBallotNum, GLOBAL_CONFIG.ids.first())

    /** Slot -> most recent AcceptProposal */
    private val accepted = hashMapOf<Int, AcceptProposal>()

    public fun receiveMessage(message: AcceptorMessage) {
        when (message) {
            is PhaseOneRequest -> {
                if (message.ballotNum > ballotNumber) {
                    ballotNumber = message.ballotNum
                    NodeLogger.logProtocol("ACCEPTOR ADOPTED $ballotNumber")
                }
                send(message.fromId, PhaseOneResponse(id, message.ballotNum, ballotNumber, accepted.values()))
            }
            is PhaseTwoRequest -> {
                if (message.payload.ballotNum > ballotNumber)
                    ballotNumber = message.payload.ballotNum
                if (message.payload.ballotNum == ballotNumber)
                    accepted[message.payload.slot] = message.payload
                send(message.fromId, PhaseTwoResponse(id, ballotNumber, message.payload))
            }
            is SlotOutMessage -> {
                val minSlotOut = slotOuts.values().min()
                val replicaId = message.fromId
                slotOuts[replicaId] = message.slotOut
                val newMinSlotOut = slotOuts.values().min()
                if (newMinSlotOut != minSlotOut)
                    cleanup()
            }
        }
    }

    private val slotOuts = HashMap(replicaIds.map{ it to 0}.toMap())

    private fun cleanup() {
        val slot = slotOuts.values().min()!!
        NodeLogger.logProtocol("ACCEPTOR CLEANUP to slotOut $slot")
        for (i in accepted.keySet().filter{ it < slot }) {
            accepted remove i
        }
    }
}

/**
 * Represents pvalue <b, s, c> of Multi-Paxos protocol.
 *
 * See [Paxos Made Moderately Complex]
 * [http://www.cs.cornell.edu/courses/cs7412/2011sp/paxos.pdf]
 */
public data class AcceptProposal(val ballotNum: Ballot, val slot: Int, val command: OperationDescriptor) {
    override fun toString(): String = "$ballotNum $slot $command"

    companion object {
        public fun parse(parts: List<String>): AcceptProposal =
                AcceptProposal(Ballot.parse(parts[0]), parts[1].toInt(), OperationDescriptor.parse(parts[2..parts.lastIndex]))
    }
}