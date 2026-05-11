package com.example.llmproxy.controller;

import com.example.llmproxy.dto.ChatRequest;
import com.example.llmproxy.dto.Message;
import com.example.llmproxy.service.LlmService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;

import java.util.List;

@RestController
@RequestMapping("/api/chat")
@RequiredArgsConstructor
@Slf4j
public class ChatController {

    private final LlmService llmService;

    @PostMapping(
            consumes = MediaType.APPLICATION_JSON_VALUE,
            produces = MediaType.TEXT_EVENT_STREAM_VALUE
    )
    public Flux<String> chat(@RequestBody ChatRequest request) {
        List<Message> messages = request.getMessages();
        int messageCount = messages == null ? 0 : messages.size();
        Message lastMessage = messageCount > 0 ? messages.get(messageCount - 1) : null;

        log.info(
                "Received /api/chat request: model={}, messageCount={}, temperature={}, lastRole={}, lastContentPreview={}",
                request.getModel(),
                messageCount,
                request.getTemperature(),
                lastMessage != null ? lastMessage.getRole() : "none",
                previewContent(lastMessage != null ? lastMessage.getContent() : null)
        );

        return Flux.defer(() -> llmService.streamChat(request))
                .onErrorResume(Throwable.class, error -> {
                    String message = error instanceof ResponseStatusException responseStatusException
                            ? responseStatusException.getReason()
                            : error.getMessage();
                    log.warn("Streaming chat failed, returning SSE error event: {}", message, error);
                    return errorEvent(message);
                });
    }

    private Flux<String> errorEvent(String message) {
        return Flux.just(
                """
                {"id":"proxy-error","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"[Error] %s"},"finish_reason":"stop"}]}
                """.formatted(escapeJson(message)),
                "[DONE]"
        );
    }

    private String previewContent(Object content) {
        if (content == null) {
            return "null";
        }

        String text = content.toString().replaceAll("\\s+", " ").trim();
        return text.length() <= 120 ? text : text.substring(0, 120) + "...";
    }

    private String escapeJson(String value) {
        if (value == null) {
            return "Unknown error";
        }

        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\r", "\\r")
                .replace("\n", "\\n")
                .replace("\t", "\\t");
    }
}
