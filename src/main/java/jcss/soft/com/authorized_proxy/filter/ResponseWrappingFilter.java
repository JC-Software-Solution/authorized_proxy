package jcss.soft.com.authorized_proxy.filter;


import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.http.server.reactive.ServerHttpResponseDecorator;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Component
@RequiredArgsConstructor
@Slf4j
public class ResponseWrappingFilter implements GlobalFilter, Ordered {

    private final ObjectMapper objectMapper;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {

        ServerHttpResponse originalResponse = exchange.getResponse();
        DataBufferFactory bufferFactory = originalResponse.bufferFactory();
        log.info("ORIGINAL RESPONSE: {}", originalResponse);
        log.info("ORIGINAL RESPONSE HEADERS: {}", originalResponse.getHeaders());

        ServerHttpResponseDecorator decoratedResponse = new ServerHttpResponseDecorator(originalResponse) {

            @Override
            public Mono<Void> writeWith(Publisher<? extends DataBuffer> body) {

                log.info("Decorated response writeWith called. Body publisher: {}", body);

                return DataBufferUtils.join(Flux.from(body))
                        .flatMap(dataBuffer -> {
                            log.info("Joined response body DataBuffer: {}", dataBuffer);

                            byte[] bytes = new byte[dataBuffer.readableByteCount()];
                            dataBuffer.read(bytes);

                            log.info("Original response body as string: {}", new String(bytes, StandardCharsets.UTF_8));

                            DataBufferUtils.release(dataBuffer);

                            String originalBody = new String(bytes, StandardCharsets.UTF_8);

                            log.info("Wrapping original body in new JSON structure...");

                            Map<String, Object> wrapped = Map.of(
                                    "request", Map.of(
                                            "data", originalBody
                                    )
                            );

                            byte[] newBody;
                            try {
                                newBody = objectMapper.writeValueAsBytes(wrapped);
                            } catch (Exception e) {
                                newBody = "{\"request\":{\"data\":\"error\"}}"
                                        .getBytes(StandardCharsets.UTF_8);
                            }

                            log.info("New wrapped response body as string: {}", new String(newBody, StandardCharsets.UTF_8));
                            DataBuffer buffer = bufferFactory.wrap(newBody);
                            return super.writeWith(Mono.just(buffer));
                        });
            }
        };

        Mono<Void> result = chain.filter(exchange.mutate().response(decoratedResponse).build());
        log.info("Filter chain result: {}", result);
        return result;
    }

    @Override
    public int getOrder() {
        return -2; // run early in response chain
    }
}