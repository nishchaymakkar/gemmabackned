package com.example.llmproxy;

import com.example.llmproxy.dto.ChatRequest;
import com.example.llmproxy.dto.Message;
import com.example.llmproxy.service.LlmService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK)
class LlmProxyApplicationTests {

	private WebTestClient webTestClient;

	@MockitoBean
	private WebClient llmWebClient;

	@BeforeEach
	@SuppressWarnings({"rawtypes", "unchecked"})
	void setUp() {
		webTestClient = WebTestClient.bindToController(new com.example.llmproxy.controller.ChatController(
				new com.example.llmproxy.service.LlmService(llmWebClient)
		)).build();

		WebClient.RequestBodyUriSpec requestBodyUriSpec = org.mockito.Mockito.mock(WebClient.RequestBodyUriSpec.class);
		WebClient.RequestBodySpec requestBodySpec = org.mockito.Mockito.mock(WebClient.RequestBodySpec.class);
		WebClient.RequestHeadersSpec requestHeadersSpec = org.mockito.Mockito.mock(WebClient.RequestHeadersSpec.class);
		WebClient.ResponseSpec responseSpec = org.mockito.Mockito.mock(WebClient.ResponseSpec.class);

		when(llmWebClient.post()).thenReturn(requestBodyUriSpec);
		when(requestBodyUriSpec.uri(any(String.class))).thenReturn(requestBodySpec);
		when(requestBodySpec.contentType(any())).thenReturn(requestBodySpec);
		when(requestBodySpec.accept(any(), any(), any())).thenReturn(requestBodySpec);
		when(requestBodySpec.bodyValue(any())).thenReturn(requestHeadersSpec);
		when(requestHeadersSpec.retrieve()).thenReturn(responseSpec);
		when(responseSpec.bodyToFlux(String.class)).thenReturn(Flux.just("{\"ok\":true}"));
	}

	@Test
	void contextLoads() {
	}

	@Test
	void testTextPayloadParsing() {
		String jsonText = """
				{
				  "model": "gemma4:e4b",
				  "messages": [
				    {
				      "role": "user",
				      "content": "Tell me a joke"
				    }
				  ]
				}
				""";

		// If parsing fails, it returns 400 Bad Request. 
		// If parsing succeeds, it attempts to contact Ollama and may return 500 (if Ollama is off)
		webTestClient.post()
				.uri("/api/chat")
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(jsonText)
				.exchange()
				.expectStatus().isOk();
	}

	@Test
	void testVisionPayloadParsing() {
		String jsonVision = """
				{
				  "model": "llava",
				  "messages": [
				    {
				      "role": "user",
				      "content": [
				        {
				          "type": "text",
				          "text": "What is this?"
				        },
				        {
				          "type": "image_url",
				          "image_url": { "url": "data:image/jpeg;base64,12345" }
				        }
				      ]
				    }
				  ]
				}
				""";

		webTestClient.post()
				.uri("/api/chat")
				.contentType(MediaType.APPLICATION_JSON)
				.bodyValue(jsonVision)
				.exchange()
				.expectStatus().isOk();
	}

	@Test
	@SuppressWarnings("unchecked")
	void convertsVisionContentForNativeOllamaFallback() {
		LlmService service = new LlmService(llmWebClient);

		ChatRequest request = new ChatRequest();
		request.setModel("llava");
		request.setMessages(List.of(new Message(
				"user",
				List.of(
						Map.of("type", "text", "text", "What is this?"),
						Map.of("type", "image_url", "image_url", Map.of("url", "data:image/jpeg;base64,abc123"))
				)
		)));

		Map<String, Object> nativeRequest = service.toNativeOllamaRequest(request);
		List<Map<String, Object>> messages = assertInstanceOf(List.class, nativeRequest.get("messages"));
		Map<String, Object> firstMessage = messages.get(0);

		assertEquals("user", firstMessage.get("role"));
		assertEquals("What is this?", firstMessage.get("content"));
		assertEquals(List.of("abc123"), firstMessage.get("images"));
	}

	@Test
	@SuppressWarnings("unchecked")
	void injectsDefaultPromptForImageOnlyNativeOllamaFallback() {
		LlmService service = new LlmService(llmWebClient);

		ChatRequest request = new ChatRequest();
		request.setModel("llava");
		request.setMessages(List.of(new Message(
				"user",
				List.of(
						Map.of("type", "image_url", "image_url", Map.of("url", "data:image/jpeg;base64,onlyImage"))
				)
		)));

		Map<String, Object> nativeRequest = service.toNativeOllamaRequest(request);
		List<Map<String, Object>> messages = assertInstanceOf(List.class, nativeRequest.get("messages"));
		Map<String, Object> firstMessage = messages.get(0);

		assertEquals("Describe this image.", firstMessage.get("content"));
		assertEquals(List.of("onlyImage"), firstMessage.get("images"));
	}

	@Test
	void streamsGenericFailuresAsSseErrorChunks() {
		when(llmWebClient.post()).thenThrow(new RuntimeException("boom"));

		String jsonText = """
				{
				  "model": "gemma4:e4b",
				  "messages": [
				    {
				      "role": "user",
				      "content": "Trigger error"
				    }
				  ]
				}
				""";

		webTestClient.post()
				.uri("/api/chat")
				.contentType(MediaType.APPLICATION_JSON)
				.accept(MediaType.TEXT_EVENT_STREAM)
				.bodyValue(jsonText)
				.exchange()
				.expectStatus().isOk()
				.expectHeader().contentTypeCompatibleWith(MediaType.TEXT_EVENT_STREAM)
				.expectBody(String.class)
				.consumeWith(response -> {
					String body = response.getResponseBody();
					assertInstanceOf(String.class, body);
					assertTrue(body.contains("\"id\":\"proxy-error\""));
					assertTrue(body.contains("[Error] boom"));
					assertTrue(body.contains("[DONE]"));
				});
	}
}
