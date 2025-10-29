package ru.ifmo.ctddev.igushkin.dkvs

/**
 * Attaching a unique [operationId] to [ClientRequest]s to distinguish them.
 */
public data class OperationDescriptor private constructor(val operationId: Int,
                                                          val request: ClientRequest
) {
    public constructor(request: ClientRequest, nodeId: Int) : this(
            nextId * GLOBAL_CONFIG.nodesCount + nodeId, request
    )

    companion object {
        public fun parse(parts: List<String>): OperationDescriptor =
                OperationDescriptor(parts[0].substring(1, parts[0].length() - 1).toInt(), ClientRequest.parse(-1, parts[1..parts.lastIndex])!!);

        private volatile var nextId: Int = 0; get() = $nextId++
    }

    override fun toString() = "<$operationId> $request"
}