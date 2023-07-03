import system.MergerEnvironment
import java.util.*
import kotlin.collections.HashMap

class MergerImpl<T : Comparable<T>>(
    private val mergerEnvironment: MergerEnvironment<T>,
    prevStepBatches: Map<Int, List<T>>?
) : Merger<T> {
    private val have: MutableList<ArrayDeque<T>> = ArrayList()

    init {
        for (i in 0 until mergerEnvironment.dataHoldersCount)
            have.add(ArrayDeque(prevStepBatches?.get(i) ?: mergerEnvironment.requestBatch(i)))
    }

    override fun mergeStep(): T? {
        val (index, result) = have.withIndex().filter { it.value.isNotEmpty() }.minByOrNull { it.value.first }
            ?: return null
        val answer = result.removeFirst()
        if (result.isEmpty())
            result.addAll(mergerEnvironment.requestBatch(index))
        return answer
    }

    override fun getRemainingBatches(): Map<Int, List<T>> {
        return have.mapIndexed { i, deque -> Pair(i, ArrayList(deque)) }.filter { it.second.isNotEmpty() }.toMap()
    }
}