package com.dineshsawant.simplymigrate.collections

interface BatchProcessor<T> {
    fun enqueue(input: T)
    fun enqueue(input: List<T>)
    fun size(): Int
    fun flush()
    fun getFlushCount(): Long
}