package com.dineshsawant.simplymigrate.util

import mu.KotlinLogging

private val logger = KotlinLogging.logger {}
class ArrayBatchProcessor<T>(private val batchSize: Int, private val processor: (List<T>) -> Unit):BatchProcessor<T> {

    var elements = ArrayList<T>(batchSize)
    private var totalFlushCount: Long = 0L

    override fun enqueue(input: T) {
        if (elements.size == batchSize - 1) {
            elements.add(input)
            flush()
        } else {
            elements.add(input)
        }
    }

    override fun enqueue(inputList: List<T>) {
        if (inputList.isEmpty()) return

        elements.addAll(inputList)
        elements.chunked(batchSize).forEach{
            elements.clear()
            elements.addAll(it)
            if (elements.size == batchSize) {
                flush()
            }
        }
    }

    override fun flush() {
        if (elements.isEmpty()) return

        logger.debug { "Flushing ${elements.size} elements" }
        processor.invoke(elements)
        totalFlushCount += elements.size.toLong()
        logger.debug { "Total flushed elements till now ${getFlushCount()}" }

        elements = ArrayList(batchSize)
    }

    override fun getFlushCount() = totalFlushCount
    override fun size() = elements.size
}
