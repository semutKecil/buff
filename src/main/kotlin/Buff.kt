package com.kawanansemut.buff

import io.reactivex.rxjava3.schedulers.Schedulers
import io.reactivex.rxjava3.subjects.PublishSubject
import java.time.LocalDateTime
import java.util.concurrent.atomic.AtomicBoolean
import java.util.logging.Logger

class Buff<T> private constructor(private val name: String, initialData: T, private val reloadFunction: (T) -> T) {
    private var data = initialData
    private var needReload = AtomicBoolean(true)
    private var processing = AtomicBoolean(false)
    private var lastModified = LocalDateTime.now()
    private val logger = Logger.getLogger(Buff.javaClass.name)


    init {
        mutableMapProcessing[name]!!.subscribeOn(Schedulers.newThread()).subscribe { processing = AtomicBoolean(it) }
    }

    private fun waitRunningProcess() {
        if (processing.get()) {
            mutableMapProcessing[name]!!.filter { !it }.subscribeOn(Schedulers.newThread()).blockingFirst()
        }
    }

    private fun processingData(doProcess: () -> Unit) {
        mutableMapProcessing[name]!!.onNext(true)
        try {
            doProcess()
        } finally {
            mutableMapProcessing[name]!!.onNext(false)
        }

    }

    fun clear() {
        waitRunningProcess()
        processingData {
            logger.info("clear buffer $name")
            needReload = AtomicBoolean(true)
        }
    }

    fun modify(modifiedData: T) {
        waitRunningProcess()
        processingData {
            logger.info("modify data buffer $name")
            data = modifiedData
            lastModified = LocalDateTime.now()
            needReload = AtomicBoolean(false)
        }
    }

    fun load(): T {
        if (needReload.get()) {
            if (!processing.get()) {
                processingData {
                    logger.info("reload buffer $name")
                    data = reloadFunction(data)
                    lastModified = LocalDateTime.now()
                    needReload = AtomicBoolean(false)
                }
            } else {
                waitRunningProcess()
            }
        }
        return data
    }

    companion object {
        private val mutableMapProcessing = mutableMapOf<String, PublishSubject<Boolean>>()
        private val mutableMapBuffer = mutableMapOf<String, Buff<*>>()
        fun <T> register(name: String, initialData: T, reloadFunction: (T) -> T) {
            mutableMapProcessing[name] = PublishSubject.create()
            mutableMapBuffer[name] = Buff(name, initialData, reloadFunction)
        }

        fun <T> get(name: String): Buff<T> {
            val buf = mutableMapBuffer[name] ?: throw Exception("buffer not registered")
            @Suppress("UNCHECKED_CAST")
            return buf as Buff<T>
        }
    }
}