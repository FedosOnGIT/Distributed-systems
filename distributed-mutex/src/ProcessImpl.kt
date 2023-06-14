package mutex

import java.rmi.UnexpectedException

/**
 * Distributed mutual exclusion implementation.
 * All functions are called from the single main thread.
 *
 * @author Надуткин Федор
 */
class ProcessImpl(private val env: Environment) : Process {
    private val inf = Int.MAX_VALUE
    private var inCS = false // are we in critical section?
    private var wantToCS = false
    private val pendingOk = BooleanArray(env.nProcesses + 1) // pending OK message (to send on unlock)
    private val forks: Array<Fork> = Array(env.nProcesses + 1) { i ->
        if (i >= env.processId) Fork.Clean
        else Fork.Missing
    }

    override fun onMessage(srcId: Int, message: Message) {
        message.parse {
            when (readEnum<MsgType>()) {
                MsgType.Req -> {
                    if (!inCS)
                        when (forks[srcId]) {
                            Fork.Clean -> {
                                if (wantToCS)
                                    pendingOk[srcId] = true
                                else {
                                    forks[srcId] = Fork.Missing
                                    send(srcId, MsgType.Ok)
                                }
                            }
                            Fork.Dirty -> {
                                forks[srcId] = Fork.Missing
                                send(srcId, MsgType.Ok)
                                if (wantToCS) send(srcId, MsgType.Req)
                            }
                            Fork.Missing -> throw UnexpectedException("You already have fork")
                        }
                    else
                        pendingOk[srcId] = true
                }
                MsgType.Ok -> forks[srcId] = Fork.Clean
            }
            checkCSEnter()
        }
    }

    private fun checkCSEnter() {
        if (!wantToCS) return // We don't need to go to CS
        if (inCS) return // already there
        for (i in 1..env.nProcesses) {
            if (i != env.processId)
                if (forks[i] == Fork.Missing) return // we don't have it
        }
        getInCS()
    }

    override fun onLockRequest() {
        check(!wantToCS) { "Lock was already requested" }
        wantToCS = true
        var have = 0 // if you have all forks, go in CS
        for (i in 1..env.nProcesses)
            if (i != env.processId) {
                if (forks[i] == Fork.Missing)
                    send(i, MsgType.Req)
                else
                    have++
            }
        if (have == env.nProcesses - 1) getInCS()
    }

    private fun getInCS() {
        inCS = true
        wantToCS = false
        env.locked()
    }

    override fun onUnlockRequest() {
        check(inCS) { "We are not in critical section" }
        env.unlocked()
        inCS = false
        for (i in 1..env.nProcesses) {
            if (pendingOk[i]) {
                pendingOk[i] = false
                forks[i] = Fork.Missing
                send(i, MsgType.Ok)
            } else
                forks[i] = Fork.Dirty
        }
    }

    private fun send(id: Int, type: MsgType) = env.send(id) { writeEnum(type) }

    enum class Fork { Clean, Dirty, Missing }

    enum class MsgType { Req, Ok }
}
