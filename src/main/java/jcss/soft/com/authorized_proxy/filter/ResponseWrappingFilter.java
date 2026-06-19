package jcss.soft.com.authorized_proxy.filter;


import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.core.Ordered;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;
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
public class ResponseWrappingFilter implements GlobalFilter, Ordered {

    private final ObjectMapper objectMapper;

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {

        ServerHttpResponse originalResponse = exchange.getResponse();
        DataBufferFactory bufferFactory = originalResponse.bufferFactory();

        ServerHttpResponseDecorator decoratedResponse = new ServerHttpResponseDecorator(originalResponse) {

            @Override
            public Mono<Void> writeWith(org.reactivestreams.Publisher<? extends DataBuffer> body) {

                Flux<? extends DataBuffer> fluxBody = Flux.from(body);

                return super.writeWith(
                        fluxBody.collectList().flatMap(dataBuffers -> {

                            StringBuilder bodyString = new StringBuilder();

                            dataBuffers.forEach(dataBuffer -> {
                                byte[] bytes = new byte[dataBuffer.readableByteCount()];
                                dataBuffer.read(bytes);
                                bodyString.append(new String(bytes, StandardCharsets.UTF_8));
                            });

                            // Wrap original response
                            Map<String, Object> wrapped = Map.of(
                                    "request", Map.of(
                                            "data", bodyString.toString()
                                    )
                            );

                            byte[] newBody;
                            try {
                                newBody = objectMapper.writeValueAsBytes(wrapped);
                            } catch (Exception e) {
                                newBody = ("{\"request\":{\"data\":\"error wrapping response\"}}")
                                        .getBytes(StandardCharsets.UTF_8);
                            }

                            DataBuffer buffer = bufferFactory.wrap(newBody);
                            return Mono.just(buffer);
                        })
                );
            }
        };

        return chain.filter(exchange.mutate().response(decoratedResponse).build());
    }

    @Override
    public int getOrder() {
        return -2; // run early in response chain
    }
}