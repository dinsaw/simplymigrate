package com.dineshsawant.datamig.driver

interface Migration {
    fun start(): Long
}