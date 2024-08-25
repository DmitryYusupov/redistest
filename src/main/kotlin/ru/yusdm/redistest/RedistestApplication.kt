package ru.yusdm.redistest

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class RedistestApplication

fun main(args: Array<String>) {
	runApplication<RedistestApplication>(*args)
}
