package com.dineshsawant.simplymigrate.driver

interface Migration {
    fun start(): Long
}