package ru.yusdm.redistest.api

import com.google.common.util.concurrent.ThreadFactoryBuilder
import io.netty.handler.timeout.ReadTimeoutHandler
import kotlinx.coroutines.*
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpStatus
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.core.publisher.Mono
import reactor.netty.Connection
import reactor.netty.http.client.HttpClient
import ru.yusdm.redistest.common.utils.HttpResponseWithNotNullableData
import ru.yusdm.redistest.common.utils.info
import ru.yusdm.redistest.common.utils.logger
import ru.yusdm.redistest.common.utils.postBlockingAsHttpResponse
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger


@RestController
class TestController(webClientBuilder: WebClient.Builder) {

    private val log = TestController::class.logger

    private val webClient = buildWebClient(webClientBuilder)

    private val threadFactory  = ThreadFactoryBuilder()
        .setNameFormat("MyCustomThread-%d").build();
    var executorService = Executors.newFixedThreadPool(25, threadFactory)

    @PostMapping("/api/get-data")
    fun getData(@RequestBody request: GetDataRequest): GetDataResponse {
        return GetDataResponse(accountId = request.accountId)
    }

    @PostMapping("/api/get-data-start-sync")
    fun getDataStart(@RequestBody request: FetchDataRequest) {
        val counter = AtomicInteger(0)
        startNCoroutinesSpreadingInTime(
            millis = request.processDurationMillis,
            coroutinesNumber = request.numberOfRequests
        ) { redisRequest ->
            val response: HttpResponseWithNotNullableData<GetDataResponse> =
                webClient.postBlockingAsHttpResponse(
                    uri = "/api/get-data",
                    body = redisRequest,
                    parameterizedTypeReference = object : ParameterizedTypeReference<GetDataResponse>() {}
                )

            counter.incrementAndGet()
            //log.info { "Counter = $counter" }
            when (response) {
                is HttpResponseWithNotNullableData.HttpErrorResponse -> {
                    log.info { "Error while exec request to fetch data" }
                }

                is HttpResponseWithNotNullableData.HttpSuccessfulResponse -> {
                    val getDataResponse: GetDataResponse = response.body
                    log.info { " ${Thread.currentThread().name} Response = $getDataResponse" }
                }
            }

        }
    }

    /**
     * Problematic
     *
     *  org.springframework.web.reactive.function.client.WebClientRequestException: Pending acquire queue has reached its maximum size of 1000
     */
    @PostMapping("/api/get-data-start-sync2")
    fun getDataStart2(@RequestBody request: FetchDataRequest) {
        val counter = AtomicInteger(0)
        startNCoroutinesSpreadingInTime(
            millis = request.processDurationMillis,
            coroutinesNumber = request.numberOfRequests
        ) { redisRequest ->

            webClient.post()
                .uri("/api/get-data")
                .bodyValue(redisRequest)
                .retrieve()
                .bodyToMono<GetDataResponse>().subscribe { response ->
                    counter.incrementAndGet()
                  log.info { " ${Thread.currentThread().name} Response = $response" }
                }
        }
    }

    private inline fun startNCoroutinesSpreadingInTime(
        millis: Long,
        coroutinesNumber: Int,
        crossinline body: suspend (redisRequest: GetDataRequest) -> Unit
    ) {
        val delayPerCoroutine = (Math.round(millis * 1f / coroutinesNumber)).toLong()

        runBlocking {
            val jobs = (1..coroutinesNumber).map {
                async(executorService.asCoroutineDispatcher(), start = CoroutineStart.LAZY) {
                    delay(delayPerCoroutine * it)
                    body(GetDataRequest(accountId = it.toLong()))
                }
            }
            jobs.awaitAll()
            log.info("Coroutines is jobs are ready")
        }
    }

    private fun buildWebClient(webClientBuilder: WebClient.Builder): WebClient {
        val client: HttpClient = HttpClient.create()
            .responseTimeout(Duration.ofSeconds(10))

        client.doOnConnected { conn: Connection ->
            conn.addHandlerLast(ReadTimeoutHandler(10, TimeUnit.SECONDS))
        }

        val webClient = webClientBuilder
            .baseUrl("http://localhost:8080")
            .defaultStatusHandler({ status ->
                status.value() == HttpStatus.NOT_FOUND.value()
            }) {
                Mono.empty()
            }
            .clientConnector(ReactorClientHttpConnector(client))
            .build()

        return webClient
    }
}

data class FetchDataRequest(
    val processDurationMillis: Long,
    val numberOfRequests: Int
)

data class GetDataRequest(
    val accountId: Long
)

data class GetDataResponse(
    val accountId: Long
)