package ru.yusdm.redistest.common.utils

import io.netty.handler.ssl.SslContext
import io.netty.handler.ssl.SslContextBuilder
import io.netty.handler.ssl.util.InsecureTrustManagerFactory
import io.netty.resolver.DefaultAddressResolverGroup
import org.springframework.core.ParameterizedTypeReference
import org.springframework.http.HttpStatus
import org.springframework.http.client.reactive.ReactorClientHttpConnector
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import reactor.core.publisher.Mono
import reactor.netty.http.client.HttpClient


private object WebClientUtilsObject {
    val log = WebClientUtilsObject::class.logger
}

fun createSslInsecureReactorClientHttpConnector(): ReactorClientHttpConnector = ReactorClientHttpConnector(
    HttpClient.create()
        .secure { t -> t.sslContext(sslInsecureContext) }
        .resolver(DefaultAddressResolverGroup.INSTANCE)
)

val sslInsecureContext: SslContext = SslContextBuilder
    .forClient()
    .trustManager(InsecureTrustManagerFactory.INSTANCE)
    .build()


inline fun <reified OUT : Any> WebClient.postBlockingWithNullableResult(
    uri: String,
    headers: Map<String, String?> = emptyMap(),
    body: Any? = null
): OUT? {
    return if (body != null) {
        this.post().uri(uri).bodyValue(body)
    } else {
        this.post().uri(uri)
    }.headers { h -> headers.forEach { (hKey, hValue) -> h.set(hKey, hValue) } }
        .retrieve()
        .bodyToMono<OUT>()
        .block()
}

inline fun <reified OUT : Any> WebClient.postBlocking(
    uri: String,
    headers: Map<String, String?> = emptyMap(),
    body: Any? = null
): OUT {
    return this.postBlockingWithNullableResult(uri, headers, body)!!
}

inline fun <reified T : Any> WebClient.getBlockingWithNullableResult(
    uri: String,
    headers: Map<String, String?> = emptyMap()
): T? {
    return this.get()
        .uri(uri)
        .headers { h -> headers.forEach { (hKey, hValue) -> h.set(hKey, hValue) } }
        .retrieve()
        .bodyToMono(T::class.java)
        .block()
}

inline fun <reified T : Any> WebClient.getBlocking(
    uri: String,
    headers: Map<String, String?> = emptyMap()
): T {
    return this.getBlockingWithNullableResult(uri, headers)!!
}

fun <T : Any> WebClient.getBlockingWithNullableResult(
    uri: String,
    headers: Map<String, String?> = emptyMap(),
    parameterizedTypeReference: ParameterizedTypeReference<T>
): T? {
    return this.get()
        .uri(uri)
        .headers { h -> headers.forEach { (hKey, hValue) -> h.set(hKey, hValue) } }
        .retrieve()
        .bodyToMono(parameterizedTypeReference)
        .block()
}

fun <T : Any> WebClient.getBlockingWithNullableResultAsHttpResponse(
    uri: String,
    headers: Map<String, String?> = emptyMap(),
    parameterizedTypeReference: ParameterizedTypeReference<T>
): HttpResponseWithNullableData<T> {
    return this.get()
        .uri(uri)
        .headers { h -> headers.forEach { (hKey, hValue) -> h.set(hKey, hValue) } }
        .exchangeToMono {
            val httpStatus = HttpStatus.valueOf(it.statusCode().value())

            if (it.statusCode().is2xxSuccessful) {

                when (val statusValue = it.statusCode().value()) {
                    HttpStatus.OK.value() -> {
                        it.bodyToMono(parameterizedTypeReference).map { data ->
                            HttpResponseWithNullableData.HttpSuccessfulResponse(status = httpStatus, body = data)
                        }
                    }
                    HttpStatus.NO_CONTENT.value() -> {
                        Mono.just(HttpResponseWithNullableData.HttpSuccessfulResponse(status = httpStatus))
                    }
                    else -> {
                        WebClientUtilsObject.log.warn("Status is not 200, but $statusValue, by default try to convert body")
                        it.bodyToMono(parameterizedTypeReference).map { data ->
                            HttpResponseWithNullableData.HttpSuccessfulResponse(status = httpStatus, body = data)
                        }
                    }
                }
            } else {
                it.createException().flatMap {
                    Mono.just(HttpResponseWithNullableData.HttpErrorResponse(status = httpStatus))
                }
            }
        }.block() ?: run {
        HttpResponseWithNullableData.HttpSuccessfulResponse(status = HttpStatus.OK)
    }
}

fun <T : Any> WebClient.postBlockingAsHttpResponse(
    uri: String,
    headers: Map<String, String?> = emptyMap(),
    body: Any? = null,
    parameterizedTypeReference: ParameterizedTypeReference<T>
): HttpResponseWithNotNullableData<T> {

    return if (body != null) {
        this.post().uri(uri).bodyValue(body)
    } else {
        this.post().uri(uri)
    }
        .headers { h -> headers.forEach { (hKey, hValue) -> h.set(hKey, hValue) } }
        .exchangeToMono {
            val httpStatus = HttpStatus.valueOf(it.statusCode().value())
            if (it.statusCode() == HttpStatus.OK) {
                it.bodyToMono(parameterizedTypeReference).map { data ->
                    HttpResponseWithNotNullableData.HttpSuccessfulResponse(status = httpStatus, body = data)
                }
            } else {
                it.createException().flatMap {
                    Mono.just(HttpResponseWithNotNullableData.HttpErrorResponse(status = httpStatus))
                }
            }
        }.block()!!
}

fun <T : Any> WebClient.getBlockingAsHttpResponse(
    uri: String,
    headers: Map<String, String?> = emptyMap(),
    parameterizedTypeReference: ParameterizedTypeReference<T>
): HttpResponseWithNotNullableData<T> {
    return this.get()
        .uri(uri)
        .headers { h -> headers.forEach { (hKey, hValue) -> h.set(hKey, hValue) } }
        .exchangeToMono {
            val httpStatus = HttpStatus.valueOf(it.statusCode().value())
            if (it.statusCode() == HttpStatus.OK) {
                it.bodyToMono(parameterizedTypeReference).map { data ->
                    HttpResponseWithNotNullableData.HttpSuccessfulResponse(status = httpStatus, body = data)
                }
            } else {
                it.createException().flatMap {
                    Mono.just(HttpResponseWithNotNullableData.HttpErrorResponse(status = httpStatus))
                }
            }
        }.block()!!
}

fun <T : Any> WebClient.getBlocking(
    uri: String,
    headers: Map<String, String?> = emptyMap(),
    parameterizedTypeReference: ParameterizedTypeReference<T>
): T {
    return this.getBlockingWithNullableResult(uri, headers, parameterizedTypeReference)!!
}

fun <T : Any> WebClient.getBlockingList(
    uri: String,
    headers: Map<String, String?> = emptyMap(),
    parameterizedTypeReference: ParameterizedTypeReference<List<T>>
): List<T> {
    return this.get()
        .uri(uri)
        .headers { h -> headers.forEach { (hKey, hValue) -> h.set(hKey, hValue) } }
        .retrieve()
        .bodyToMono(parameterizedTypeReference)
        .block() ?: emptyList()
}

fun <T : Any> WebClient.getBlockingListAsHttpResponse(
    uri: String,
    headers: Map<String, String?> = emptyMap(),
    parameterizedTypeReference: ParameterizedTypeReference<List<T>>
): HttpResponseWithNotNullableData<List<T>> {
    return this.get()
        .uri(uri)
        .headers { h -> headers.forEach { (hKey, hValue) -> h.set(hKey, hValue) } }
        .exchangeToMono {
            val httpStatus = HttpStatus.valueOf(it.statusCode().value())

            if (it.statusCode().is2xxSuccessful) {

                when (val statusValue = it.statusCode().value()) {
                    HttpStatus.OK.value() -> {
                        it.bodyToMono(parameterizedTypeReference).map { data ->
                            HttpResponseWithNotNullableData.HttpSuccessfulResponse(status = httpStatus, body = data)
                        }
                    }
                    HttpStatus.NO_CONTENT.value() -> {
                        Mono.just(
                            HttpResponseWithNotNullableData.HttpSuccessfulResponse(
                                status = httpStatus,
                                body = emptyList()
                            )
                        )
                    }
                    else -> {
                        WebClientUtilsObject.log.warn("Status is not 200, but $statusValue, by default try to convert body")
                        it.bodyToMono(parameterizedTypeReference).map { data ->
                            HttpResponseWithNotNullableData.HttpSuccessfulResponse(status = httpStatus, body = data)
                        }
                    }
                }

            } else {
                it.createException().flatMap {
                    Mono.just(HttpResponseWithNotNullableData.HttpErrorResponse(status = httpStatus))
                }
            }
        }.block() ?: HttpResponseWithNotNullableData.HttpSuccessfulResponse(
        status = HttpStatus.OK,
        body = emptyList()
    )
}

sealed class HttpResponseWithNotNullableData<T>(open val status: HttpStatus) {
    data class HttpErrorResponse<T>(override val status: HttpStatus, val message: String? = null) :
        HttpResponseWithNotNullableData<T>(status)

    data class HttpSuccessfulResponse<T>(override val status: HttpStatus, val body: T) :
        HttpResponseWithNotNullableData<T>(status)
}

sealed class HttpResponseWithNullableData<T>(open val status: HttpStatus) {
    data class HttpErrorResponse<T>(override val status: HttpStatus, val message: String? = null) :
        HttpResponseWithNullableData<T>(status)

    data class HttpSuccessfulResponse<T>(override val status: HttpStatus, val body: T? = null) :
        HttpResponseWithNullableData<T>(status)
}