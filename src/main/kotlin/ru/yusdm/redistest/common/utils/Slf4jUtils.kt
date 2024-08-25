package ru.yusdm.redistest.common.utils

import kotlin.reflect.KClass

import org.slf4j.Logger
import org.slf4j.LoggerFactory

val KClass<*>.logger: Logger get() = LoggerFactory.getLogger(java)

inline fun Logger.trace(lazyMessage: () -> Any?): Unit {
    if (isTraceEnabled) {
        val message = lazyMessage()
        trace(message as? String ?: message?.toString())
    }
}

inline fun Logger.trace(exception: Throwable?, lazyMessage: () -> Any?): Unit {
    if (isTraceEnabled) {
        val message = lazyMessage()
        trace(message as? String ?: message?.toString(), exception)
    }
}

inline fun Logger.debug(lazyMessage: () -> Any?): Unit {
    if (isDebugEnabled) {
        val message = lazyMessage()
        debug(message as? String ?: message?.toString())
    }
}

inline fun Logger.debug(exception: Throwable?, lazyMessage: () -> Any?): Unit {
    if (isDebugEnabled) {
        val message = lazyMessage()
        debug(message as? String ?: message?.toString(), exception)
    }
}

inline fun Logger.info(lazyMessage: () -> Any?): Unit {
    if (isInfoEnabled) {
        val message = lazyMessage()
        info(message as? String ?: message?.toString())
    }
}

inline fun Logger.info(exception: Throwable?, lazyMessage: () -> Any?): Unit {
    if (isInfoEnabled) {
        val message = lazyMessage()
        info(message as? String ?: message?.toString(), exception)
    }
}

inline fun Logger.warn(lazyMessage: () -> Any?): Unit {
    if (isWarnEnabled) {
        val message = lazyMessage()
        warn(message as? String ?: message?.toString())
    }
}

inline fun Logger.warn(exception: Throwable?, lazyMessage: () -> Any?): Unit {
    if (isWarnEnabled) {
        val message = lazyMessage()
        warn(message as? String ?: message?.toString(), exception)
    }
}

inline fun Logger.error(lazyMessage: () -> Any?): Unit {
    if (isErrorEnabled) {
        val message = lazyMessage()
        error(message as? String ?: message?.toString())
    }
}

inline fun Logger.error(exception: Throwable?, lazyMessage: () -> Any?): Unit {
    if (isErrorEnabled) {
        val message = lazyMessage()
        error(message as? String ?: message?.toString(), exception)
    }
}