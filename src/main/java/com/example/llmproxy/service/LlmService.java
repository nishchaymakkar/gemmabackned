package com.example.llmproxy.service;

import com.example.llmproxy.dto.ChatRequest;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

@Service
@RequiredArgsConstructor
public class LlmService {

    private final WebClient llmWebClient;

    public Flux<String> streamChat(ChatRequest chatRequest) {
        // Ensure stream is true to receive chunks from the local LLM server
        chatRequest.setStream(true);

        return llmWebClient.post()
                .uri("/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(chatRequest)
                .retrieve()
                .bodyToFlux(String.class); // Reads the Server-Sent Events from Ollama/LM Studio
    }
}
