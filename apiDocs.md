# LLM Proxy Server API Documentation

This documentation outlines how your native Android app should interact with the Spring Boot backend to generate AI responses.

---

## 1. Chat Completions (Streaming)

This is the primary endpoint to send a conversation to the local LLM and receive a stream of text chunks in real-time, just like the ChatGPT Android app.

- **URL (from Emulator):** `http://10.0.2.2:8080/api/chat` *(Note: `10.0.2.2` points to your computer's localhost from inside the Android Emulator)*
- **Method:** `POST`
- **Content-Type:** `application/json`
- **Produces:** `text/event-stream` (Server-Sent Events)

### Request Body Payload

The payload follows the standard OpenAI API format. 

```json
{
  "model": "local-model",
  "temperature": 0.7,
  "stream": true,
  "messages": [
    {
      "role": "system",
      "content": "You are a helpful AI assistant."
    },
    {
      "role": "user",
      "content": "Write a short poem about the ocean."
    }
  ]
}
```

| Field | Type | Description | Default |
|-------|------|-------------|---------|
| `model` | String | The identifier of the model to use. | `"local-model"` |
| `temperature` | Float | Controls randomness (0.0 to 1.0). Higher is more creative. | `0.7` |
| `stream` | Boolean | Must be true to receive SSE streaming chunks. | `true` |
| `messages` | Array | The conversation history. Each object requires `role` (system, user, or assistant) and `content`. | **Required** |

---

### Response Format

Because this endpoint produces a `text/event-stream`, the response will not be a single JSON object. Instead, the backend will continuously push data chunks to the Android app.

```text
data: {"id":"chatcmpl-123","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"The "}}]}
data: {"id":"chatcmpl-123","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"ocean "}}]}
data: [DONE]
```

---

## 2. Android Implementation Example (Kotlin + OkHttp SSE)

To correctly consume Server-Sent Events in a native Android app, you should use the official `okhttp-sse` library.

### Step A: Add Dependencies (build.gradle.kts)
```kotlin
dependencies {
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("com.squareup.okhttp3:okhttp-sse:4.12.0")
    implementation("com.google.code.gson:gson:2.10.1") // Or use Kotlinx Serialization
}
```

### Step B: Kotlin Implementation
Here is a drop-in example of how to make the request and process the streaming tokens to update your UI:

```kotlin
import okhttp3.*
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.sse.EventSource
import okhttp3.sse.EventSourceListener
import okhttp3.sse.EventSources
import com.google.gson.JsonParser
import java.util.concurrent.TimeUnit

fun streamChatFromBackend(userMessage: String) {
    // 1. Setup OkHttp client. Disable read timeout since streams can last a while.
    val client = OkHttpClient.Builder()
        .readTimeout(0, TimeUnit.MILLISECONDS) 
        .build()

    // 2. Build the JSON Payload
    val jsonMediaType = "application/json; charset=utf-8".toMediaType()
    val jsonPayload = """
        {
          "messages": [{"role": "user", "content": "$userMessage"}],
          "stream": true
        }
    """.trimIndent()
    val body = jsonPayload.toRequestBody(jsonMediaType)

    // 3. Create the Request (use 10.0.2.2 if testing on Android Emulator)
    val request = Request.Builder()
        .url("http://10.0.2.2:8080/api/chat") 
        .post(body)
        .build()

    // 4. Setup the Server-Sent Events Listener
    val listener = object : EventSourceListener() {
        override fun onEvent(eventSource: EventSource, id: String?, type: String?, data: String) {
            if (data == "[DONE]") {
                println("Stream completely finished.")
                return
            }

            try {
                // Parse the chunk data
                val jsonObject = JsonParser.parseString(data).asJsonObject
                val choices = jsonObject.getAsJsonArray("choices")
                
                if (choices.size() > 0) {
                    val delta = choices.get(0).asJsonObject.getAsJsonObject("delta")
                    
                    // Check for standard content (or reasoning_content if using Gemma/DeepSeek)
                    if (delta.has("content")) {
                        val word = delta.get("content").asString
                        
                        // IMPORTANT: Update your UI on the Main Thread
                        // runOnUiThread {
                        //     chatTextView.append(word)
                        // }
                        print(word)
                    } else if (delta.has("reasoning_content")) {
                        // Optional: Handle the AI's "thought process" tokens
                        val thought = delta.get("reasoning_content").asString
                        print(thought)
                    }
                }
            } catch (e: Exception) {
                e.printStackTrace()
            }
        }

        override fun onFailure(eventSource: EventSource, t: Throwable?, response: Response?) {
            super.onFailure(eventSource, t, response)
            println("Stream failed: ${t?.message}")
        }
    }

    // 5. Start the connection!
    EventSources.createFactory(client).newEventSource(request, listener)
}
```
