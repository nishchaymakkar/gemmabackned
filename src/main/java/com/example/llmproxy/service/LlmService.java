package com.example.llmproxy.service;

import com.example.llmproxy.dto.ChatRequest;
import com.example.llmproxy.dto.Message;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Service
@RequiredArgsConstructor
@Slf4j
public class LlmService {
    private static final String DEFAULT_VISION_PROMPT = "Describe this image.";

    private final WebClient llmWebClient;
    @Value("${llm.api.base-url}")
    private String llmApiBaseUrl;

    @Value("${llm.api.default-model:}")
    private String defaultModel;

    public Flux<String> streamChat(ChatRequest chatRequest) {
        // Ensure stream is true to receive chunks from the local LLM server
        chatRequest.setStream(true);
        applyDefaultModelIfNeeded(chatRequest);
        int messageCount = chatRequest.getMessages() == null ? 0 : chatRequest.getMessages().size();

        log.info(
                "Forwarding chat request to upstream LLM: basePath=/chat/completions, model={}, messageCount={}, stream={}",
                chatRequest.getModel(),
                messageCount,
                chatRequest.isStream()
        );

        return llmWebClient.post()
                .uri("/chat/completions")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM, MediaType.APPLICATION_NDJSON, MediaType.APPLICATION_JSON)
                .bodyValue(chatRequest)
                .retrieve()
                .bodyToFlux(String.class)
                .onErrorResume(WebClientResponseException.class, error -> fallbackToNativeOllama(chatRequest, error))
                .onErrorMap(WebClientRequestException.class, error -> new ResponseStatusException(
                        HttpStatus.BAD_GATEWAY,
                        "Could not reach Ollama at %s. Check that Ollama is running and llm.api.base-url is correct."
                                .formatted(llmApiBaseUrl),
                        error
                ))
                .doOnSubscribe(subscription -> log.info("Upstream LLM stream subscribed"))
                .doOnNext(chunk -> log.debug("Received upstream chunk: {}", previewChunk(chunk)))
                .doOnComplete(() -> log.info("Upstream LLM stream completed"))
                .doOnError(error -> log.error("Upstream LLM stream failed: {}", error.getMessage(), error));
    }

    private Flux<String> fallbackToNativeOllama(ChatRequest chatRequest, WebClientResponseException error) {
        if (error.getStatusCode() != HttpStatus.NOT_FOUND) {
            return Flux.error(error);
        }

        if (isModelMissingError(error)) {
            return Flux.error(new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    missingModelMessage(chatRequest.getModel(), error),
                    error
            ));
        }

        String nativeBaseUrl = nativeBaseUrl();
        log.warn(
                "OpenAI-compatible Ollama endpoint was not found at {}. Falling back to native Ollama endpoint {}/api/chat",
                llmApiBaseUrl,
                nativeBaseUrl
        );

        return llmWebClient.mutate()
                .baseUrl(nativeBaseUrl)
                .build()
                .post()
                .uri("/api/chat")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.APPLICATION_NDJSON, MediaType.APPLICATION_JSON)
                .bodyValue(toNativeOllamaRequest(chatRequest))
                .retrieve()
                .bodyToFlux(String.class)
                .onErrorMap(WebClientResponseException.NotFound.class, nativeNotFound -> {
                    if (isModelMissingError(nativeNotFound)) {
                        return new ResponseStatusException(
                                HttpStatus.BAD_REQUEST,
                                missingModelMessage(chatRequest.getModel(), nativeNotFound),
                                nativeNotFound
                        );
                    }

                    return new ResponseStatusException(
                            HttpStatus.BAD_GATEWAY,
                            "The configured upstream at %s is not exposing Ollama chat endpoints. Tried both /v1/chat/completions and /api/chat."
                                    .formatted(llmApiBaseUrl),
                            nativeNotFound
                    );
                })
                .onErrorMap(WebClientRequestException.class, nativeRequestError -> new ResponseStatusException(
                        HttpStatus.BAD_GATEWAY,
                        "Could not reach Ollama native endpoint at %s/api/chat."
                                .formatted(nativeBaseUrl),
                        nativeRequestError
                ))
                .filter(Objects::nonNull)
                .<String>handle((chunk, sink) -> {
                    String mappedChunk = mapNativeChunkToOpenAiChunk(chunk);
                    if (mappedChunk != null) {
                        sink.next(mappedChunk);
                    }
                })
                .concatWithValues("[DONE]");
    }

    public Map<String, Object> toNativeOllamaRequest(ChatRequest chatRequest) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("model", chatRequest.getModel());
        payload.put("messages", toNativeOllamaMessages(chatRequest.getMessages()));
        payload.put("stream", true);

        Map<String, Object> options = new LinkedHashMap<>();
        options.put("temperature", chatRequest.getTemperature());
        payload.put("options", options);
        return payload;
    }

    private List<Map<String, Object>> toNativeOllamaMessages(List<Message> messages) {
        if (messages == null) {
            return List.of();
        }

        return messages.stream()
                .map(this::toNativeOllamaMessage)
                .toList();
    }

    private Map<String, Object> toNativeOllamaMessage(Message message) {
        Map<String, Object> nativeMessage = new LinkedHashMap<>();
        nativeMessage.put("role", message.getRole());

        Object content = message.getContent();
        if (content instanceof List<?> parts) {
            nativeMessage.putAll(convertVisionContent(parts));
            return nativeMessage;
        }

        nativeMessage.put("content", content == null ? "" : content.toString());
        return nativeMessage;
    }

    private Map<String, Object> convertVisionContent(List<?> parts) {
        Map<String, Object> contentMap = new LinkedHashMap<>();
        List<String> textParts = new ArrayList<>();
        List<String> images = new ArrayList<>();

        for (Object part : parts) {
            if (!(part instanceof Map<?, ?> rawPart)) {
                continue;
            }

            Object type = rawPart.get("type");
            if ("text".equals(type)) {
                Object text = rawPart.get("text");
                if (text != null && !text.toString().isBlank()) {
                    textParts.add(text.toString());
                }
                continue;
            }

            if ("image_url".equals(type)) {
                extractInlineBase64Image(rawPart).ifPresent(images::add);
            }
        }

        String combinedText = String.join("\n", textParts).trim();
        contentMap.put("content", combinedText.isBlank() ? DEFAULT_VISION_PROMPT : combinedText);
        if (!images.isEmpty()) {
            contentMap.put("images", images);
        }
        return contentMap;
    }

    private java.util.Optional<String> extractInlineBase64Image(Map<?, ?> rawPart) {
        Object imageUrlObject = rawPart.get("image_url");
        if (!(imageUrlObject instanceof Map<?, ?> imageUrlMap)) {
            return java.util.Optional.empty();
        }

        Object url = imageUrlMap.get("url");
        if (url == null) {
            return java.util.Optional.empty();
        }

        String urlValue = url.toString();
        int base64MarkerIndex = urlValue.indexOf("base64,");
        if (base64MarkerIndex < 0) {
            log.warn("Skipping non-inline image_url for native Ollama fallback");
            return java.util.Optional.empty();
        }

        String base64 = urlValue.substring(base64MarkerIndex + "base64,".length()).trim();
        return base64.isBlank() ? java.util.Optional.empty() : java.util.Optional.of(base64);
    }

    @SuppressWarnings("unchecked")
    private String mapNativeChunkToOpenAiChunk(String chunk) {
        Map<String, Object> root = parseJsonMap(chunk);
        if (root == null || root.isEmpty()) {
            return null;
        }

        Object doneValue = root.get("done");
        boolean done = doneValue instanceof Boolean && (Boolean) doneValue;
        Map<String, Object> message = root.get("message") instanceof Map<?, ?> messageMap
                ? (Map<String, Object>) messageMap
                : Map.of();
        String content = message.get("content") == null ? "" : message.get("content").toString();

        if (done && content.isBlank()) {
            return null;
        }

        return """
                {"id":"ollama-native","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"role":"assistant","content":"%s"},"finish_reason":%s}]}
                """.formatted(
                escapeJson(content),
                done ? "\"stop\"" : "null"
        ).trim();
    }

    private Map<String, Object> parseJsonMap(String chunk) {
        try {
            return new org.springframework.boot.json.JacksonJsonParser().parseMap(chunk);
        } catch (Exception error) {
            log.warn("Failed to parse native Ollama chunk: {}", previewChunk(chunk), error);
            return null;
        }
    }

    private void applyDefaultModelIfNeeded(ChatRequest chatRequest) {
        if (chatRequest.getModel() != null && !chatRequest.getModel().isBlank() && !"local-model".equals(chatRequest.getModel())) {
            return;
        }

        if (defaultModel != null && !defaultModel.isBlank()) {
            log.info("Using configured default model for request: {}", defaultModel);
            chatRequest.setModel(defaultModel);
            return;
        }

        log.warn("Request model is '{}' and no llm.api.default-model is configured. Ollama may reject the request if that model does not exist.", chatRequest.getModel());
    }

    private String nativeBaseUrl() {
        String trimmed = llmApiBaseUrl == null ? "" : llmApiBaseUrl.trim();
        if (trimmed.endsWith("/v1")) {
            return trimmed.substring(0, trimmed.length() - 3);
        }
        if (trimmed.endsWith("/")) {
            return trimmed.substring(0, trimmed.length() - 1);
        }
        return trimmed;
    }

    private String previewChunk(String chunk) {
        if (chunk == null) {
            return "null";
        }

        String text = chunk.replaceAll("\\s+", " ").trim();
        return text.length() <= 160 ? text : text.substring(0, 160) + "...";
    }

    private String escapeJson(String value) {
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }

    private boolean isModelMissingError(WebClientResponseException error) {
        String body = error.getResponseBodyAsString();
        if (body == null) {
            return false;
        }

        String normalized = body.toLowerCase();
        return normalized.contains("model") && normalized.contains("not found");
    }

    private String missingModelMessage(String model, WebClientResponseException error) {
        String body = error.getResponseBodyAsString();
        String upstreamMessage = (body == null || body.isBlank()) ? "Unknown upstream error." : body;
        return "The upstream Ollama model '%s' was not found. Install a vision-capable model such as 'llava' or change the request model. Upstream response: %s"
                .formatted(model, upstreamMessage);
    }
}
