package com.dineshsawant.simplymigrate.util

interface BatchProcessor<T> {
    fun enqueue(input: T)
    fun enqueue(inputList: List<T>)
    fun size(): Int
    fun flush()
    fun getFlushCount(): Long
}