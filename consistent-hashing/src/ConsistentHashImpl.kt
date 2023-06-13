import java.util.*
import kotlin.collections.HashMap
import kotlin.collections.HashSet

class ConsistentHashImpl<K> : ConsistentHash<K> {
    private val vNodes = TreeMap<Int, Shard>();
    private val shards = HashMap<Shard, Set<Int>>()

    private fun getLowerKey(key: Int): Int {
        var index = vNodes.lowerKey(key)
        if (index == null)
            index = vNodes.lowerKey(Int.MAX_VALUE)
        return index
    }
    private fun getHigherKey(key: Int): Int {
        var index = vNodes.higherKey(key)
        if (index == null)
            index = vNodes.higherKey(Int.MIN_VALUE)
        return index
    }
    override fun getShardByKey(key: K): Shard {
        val hash = Objects.hashCode(key);
        if (vNodes.containsKey(hash)) {
            return vNodes[hash]!!;
        }
        return vNodes[getHigherKey(hash)]!!
    }
    override fun addShard(newShard: Shard, vnodeHashes: Set<Int>): Map<Shard, Set<HashRange>> {
        shards[newShard] = vnodeHashes
        val answer = HashMap<Shard, MutableSet<HashRange>>()
        if (vNodes.size > 0) {
            val results = HashMap<Int, HashRange>()
            for (vnode in vnodeHashes) {
                val index = getHigherKey(vnode)
                if (results.contains(index)) {
                    val range = results[index]!!
                    val newDistance = (index + Int.MAX_VALUE - vnode) % Int.MAX_VALUE
                    val oldDistance = (index + Int.MAX_VALUE - range.rightBorder) % Int.MAX_VALUE
                    if (newDistance < oldDistance)
                        results[index] = HashRange(range.leftBorder, vnode)
                    continue
                }
                val start = getLowerKey(vnode);
                results[index] = HashRange(leftBorder = start + 1, rightBorder = vnode)
            }
            for (element in results) {
                val shard = vNodes[element.key]!!
                if (!answer.containsKey(shard)) {
                    answer[shard] = HashSet()
                }
                answer[shard]!!.add(element.value)
            }
        }
        for (vnode in vnodeHashes) vNodes[vnode] = newShard
        return answer
    }

    override fun removeShard(shard: Shard): Map<Shard, Set<HashRange>> {
        val answer = HashMap<Shard, MutableSet<HashRange>>()
        val nodes = shards[shard]!!
        if (shards.size != 1) {
            val were = HashSet<Int>()
            for (vnode in nodes) {
                if (were.contains(vnode)) {
                    continue
                }
                were.add(vnode)
                var finish = vnode
                var next = getHigherKey(vnode)
                while (vNodes[next] == vNodes[finish]) {
                    val temporary = getHigherKey(next)
                    finish = next
                    next = temporary
                    were.add(finish)
                }
                var start = getLowerKey(vnode)
                while (vNodes[start] == vNodes[vnode]) {
                    were.add(start)
                    start = getLowerKey(start)
                }
                val nextShard = vNodes[next]!!
                if (!answer.containsKey(nextShard)) {
                    answer[nextShard] = HashSet()
                }
                answer[nextShard]!!.add(HashRange(leftBorder = start+ 1, rightBorder = finish))
            }
        }
        for (vnode in nodes) {
            vNodes.remove(vnode)
        }
        shards.remove(shard)
        return answer
    }
}