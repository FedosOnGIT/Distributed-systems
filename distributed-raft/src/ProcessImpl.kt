package raft

import raft.Message.*
import java.lang.Integer.max
import java.lang.Integer.min
import java.util.*

/**
 * Raft algorithm implementation.
 * All functions are called from the single main thread.
 *
 * @author Федор Надуткин
 */
class ProcessImpl(private val env: Environment) : Process {
    enum class State {
        Leader, Candidate, Follower
    }

    private val storage = env.storage
    private val machine = env.machine
    private var state = State.Follower
    private var commitIndex = 0
    private val lastAdded: LogId
        get() = storage.readLastLogId()
    private val persistentState: PersistentState
        get() = storage.readPersistentState()

    private fun get(index: Int): LogEntry? = storage.readLog(index)

    private val need = ArrayDeque<Command>()

    // Follower
    private var leaderId: Int? = null

    // Candidate
    private var voted = 0

    // Leader
    private lateinit var nextIndex: Array<Int>
    private lateinit var matchIndex: Array<Int>
    private var commandResponded = true

    init {
        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }


    private fun applyChanges(new: Int): Pair<Int, CommandResult>? {
        var result: Pair<Int, CommandResult>? = null
        for (j in commitIndex + 1..new) {
            val command = get(j)!!.command
            result = Pair(command.processId, machine.apply(command))
        }
        commitIndex = new
        return result
    }

    private fun commit(): Pair<Int, CommandResult>? {
        for (i in lastAdded.index downTo commitIndex + 1) {
            val entry = get(i)
            if (entry!!.id.term != persistentState.currentTerm) {
                return null
            }
            if (matchIndex.count { last -> last >= i } > (env.nProcesses / 2))
                return applyChanges(i)
        }
        return null
    }

    private fun send(srcId: Int, heartbeat: Boolean = false) {
        val previous = if (nextIndex[srcId] <= 1) START_LOG_ID else get(nextIndex[srcId] - 1)!!.id
        val entry = if (heartbeat) null else get(nextIndex[srcId])
        env.send(
            srcId,
            AppendEntryRpc(
                term = persistentState.currentTerm,
                prevLogId = previous,
                leaderCommit = commitIndex,
                entry = entry
            )
        )
    }

    private fun broadcast(heartbeat: Boolean = false) {
        for (i in 1..env.nProcesses)
            if (i != env.processId)
                send(srcId = i, heartbeat = heartbeat)
    }

    private fun restart(term: Int) {
        storage.writePersistentState(PersistentState(term))
        state = State.Follower
        env.startTimeout(Timeout.ELECTION_TIMEOUT)
    }

    private fun leadersRoutine() {
        commandResponded = true
        broadcast(heartbeat = true)
        env.startTimeout(Timeout.LEADER_HEARTBEAT_PERIOD)
    }

    override fun onTimeout() {
        when (state) {
            State.Leader -> leadersRoutine()
            else -> {
                leaderId = null
                state = State.Candidate
                storage.writePersistentState(PersistentState(persistentState.currentTerm + 1, env.processId))
                voted = 1
                for (i in 1..env.nProcesses)
                    if (i != env.processId)
                        env.send(i, RequestVoteRpc(persistentState.currentTerm, lastAdded))
                env.startTimeout(Timeout.ELECTION_TIMEOUT)
            }
        }
    }

    private fun add(command: Command) = storage.appendLogEntry(
        LogEntry(
            id = LogId(
                index = lastAdded.index + 1,
                term = persistentState.currentTerm
            ), command = command
        )
    )

    private fun sendStored() {
        while (!need.isEmpty())
            env.send(leaderId!!, ClientCommandRpc(persistentState.currentTerm, need.removeFirst()))
    }

    private fun processCommand(command: Command) {
        if (state == State.Leader) {
            add(command = command)
            if (commandResponded)
                broadcast()
            commandResponded = false
            matchIndex[env.processId]++
            nextIndex[env.processId]++
        } else {
            need.add(command)
            if (leaderId != null) {
                sendStored()
            }
        }
    }

    override fun onMessage(srcId: Int, message: Message) {
        if (persistentState.currentTerm < message.term)
            restart(message.term)
        when (message) {
            is AppendEntryRpc -> {
                if (message.term < persistentState.currentTerm) {
                    env.send(srcId, AppendEntryResult(persistentState.currentTerm, null))
                    return
                }
                leaderId = srcId
                env.startTimeout(Timeout.ELECTION_TIMEOUT)
                sendStored()
                if (lastAdded != message.prevLogId) {
                    env.send(srcId, AppendEntryResult(persistentState.currentTerm, null))
                } else {
                    if (message.entry != null)
                        storage.appendLogEntry(message.entry)
                    applyChanges(min(message.leaderCommit, lastAdded.index))
                    env.send(srcId, AppendEntryResult(persistentState.currentTerm, lastAdded.index))
                }
            }
            is AppendEntryResult -> {
                commandResponded = true
                if (state == State.Leader) {
                    if (message.lastIndex == null) {
                        nextIndex[srcId] = max(nextIndex[srcId] - 1, matchIndex[srcId] + 1)
                        send(srcId = srcId)
                    } else if (matchIndex[srcId] < message.lastIndex) {
                        matchIndex[srcId] = message.lastIndex
                        nextIndex[srcId] = message.lastIndex + 1
                        if (message.lastIndex < lastAdded.index)
                            send(srcId = srcId)
                        val result = commit()
                        if (result != null) {
                            if (result.first == env.processId)
                                env.onClientCommandResult(result.second)
                            else
                                env.send(result.first, ClientCommandResult(persistentState.currentTerm, result.second))
                        }
                    }
                }
            }
            is RequestVoteRpc -> {
                if (persistentState.currentTerm > message.term
                    || lastAdded.term > message.lastLogId.term
                    || (lastAdded.term == message.lastLogId.term && lastAdded.index > message.lastLogId.index)
                ) {
                    env.send(srcId, RequestVoteResult(persistentState.currentTerm, voteGranted = false))
                    return
                }
                when (persistentState.votedFor) {
                    null -> {
                        storage.writePersistentState(PersistentState(message.term, votedFor = srcId))
                        env.send(srcId, RequestVoteResult(persistentState.currentTerm, voteGranted = true))
                        env.startTimeout(Timeout.ELECTION_TIMEOUT)
                    }
                    srcId -> {
                        env.send(srcId, RequestVoteResult(persistentState.currentTerm, voteGranted = true))
                    }
                    else -> {
                        env.send(srcId, RequestVoteResult(persistentState.currentTerm, voteGranted = false))
                    }
                }
            }
            is RequestVoteResult -> {
                if (state == State.Candidate && message.voteGranted) {
                    voted++
                    if (voted > env.nProcesses / 2) {
                        state = State.Leader
                        while (!need.isEmpty())
                            add(command = need.removeFirst())
                        nextIndex = Array(env.nProcesses + 1) { lastAdded.index + 1 }
                        matchIndex = Array(env.nProcesses + 1) { 0 }
                        matchIndex[env.processId] = lastAdded.index
                        leadersRoutine()
                    }
                }
            }
            is ClientCommandRpc -> processCommand(message.command)
            is ClientCommandResult -> {
                leaderId = srcId
                env.onClientCommandResult(message.result)
                sendStored()
            }
        }
    }

    override fun onClientCommand(command: Command) {
        processCommand(command)
    }
}
