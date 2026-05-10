package com.example.llmproxy.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class WebClientConfig {

    @Value("${llm.api.base-url}")
    private String llmApiBaseUrl;

    @Value("${llm.api.key:}")
    private String llmApiKey;

    @Bean
    public WebClient llmWebClient() {
        return WebClient.builder().baseUrl(llmApiBaseUrl)
                .defaultHeader("Authorization", "Bearer " + llmApiKey)
                .build();
    }
}
