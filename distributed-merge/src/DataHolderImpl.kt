import system.DataHolderEnvironment
import java.lang.Integer.min

class DataHolderImpl<T : Comparable<T>>(
    private val keys: List<T>,
    private val dataHolderEnvironment: DataHolderEnvironment
) : DataHolder<T> {
    var current = 0
    var checked = 0
    override fun checkpoint() {
        checked = current
    }

    override fun rollBack() {
        current = checked
    }

    override fun getBatch(): List<T> {
        val to = min(current + dataHolderEnvironment.batchSize, keys.size)
        val batch = keys.subList(current, to)
        current = to
        return batch
    }
}