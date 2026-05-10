package com.example.llmproxy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
class LlmProxyApplicationTests {

	@Autowired
	private WebTestClient webTestClient;

	@Test
	void contextLoads() {
	}

	@Test
	void testTextPayloadParsing() {
		String jsonText = """
				{
				  "model": "llama3",
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
				.expectStatus().is5xxServerError(); // 5xx means it parsed the JSON successfully and tried to proxy!
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
				.expectStatus().is5xxServerError(); // 5xx means it parsed the array JSON successfully too!
	}
}
