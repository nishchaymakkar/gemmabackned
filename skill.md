# Agent Skill: LLM Proxy Server Integration

This file serves as a comprehensive reference for the LLM Proxy Server architecture, detailing both the API endpoints exposed by the Spring Boot backend and the implementation guide for native Android clients.

---

## Part 1: API Documentation

# LLM Proxy Server API Documentation

This documentation outlines how your native Android app should interact with the Spring Boot backend to generate AI responses.

---

### 1. Chat Completions (Streaming)

This is the primary endpoint to send a conversation to the local LLM and receive a stream of text chunks in real-time, just like the ChatGPT Android app.

- **URL (from Emulator):** `http://10.0.2.2:8080/api/chat` *(Note: `10.0.2.2` points to your computer's localhost from inside the Android Emulator)*
- **Method:** `POST`
- **Content-Type:** `application/json`
- **Produces:** `text/event-stream` (Server-Sent Events)

#### Request Body Payload

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

#### Response Format

Because this endpoint produces a `text/event-stream`, the response will not be a single JSON object. Instead, the backend will continuously push data chunks to the Android app.

```text
data: {"id":"chatcmpl-123","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"The "}}]}
data: {"id":"chatcmpl-123","object":"chat.completion.chunk","choices":[{"index":0,"delta":{"content":"ocean "}}]}
data: [DONE]
```

---

## Part 2: Android Integration Guide

# Android Integration Guide: Streaming LLM Responses via SSE

This guide explains how to connect your native Android application to the Spring Boot AI Proxy and stream responses in real-time using Server-Sent Events (SSE).

### 1. Dependencies

Add the following to your Android app's `app/build.gradle` (or `build.gradle.kts`):

```gradle
dependencies {
    // OkHttp & SSE
    implementation("com.squareup.okhttp3:okhttp:4.12.0")
    implementation("com.squareup.okhttp3:okhttp-sse:4.12.0")
    
    // Moshi for JSON parsing (or use Gson/Kotlinx Serialization)
    implementation("com.squareup.moshi:moshi-kotlin:1.15.0")
    
    // ViewModel & Coroutines
    implementation("androidx.lifecycle:lifecycle-viewmodel-ktx:2.7.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-android:1.7.3")
}
```

### 2. Network Configuration

When testing locally, the Spring Boot proxy uses HTTP instead of HTTPS. You must allow cleartext traffic in your `AndroidManifest.xml`:

```xml
<manifest xmlns:android="http://schemas.android.com/apk/res/android">
    <uses-permission android:name="android.permission.INTERNET" />

    <application
        android:usesCleartextTraffic="true"
        ... >
    </application>
</manifest>
```

> **Note:** If running the Android Emulator targeting your local development machine, use `http://10.0.2.2:8080` as the base URL.

### 3. Data Models

Create Kotlin data classes to represent the OpenAI-compatible JSON responses from the backend:

```kotlin
import com.squareup.moshi.JsonClass

@JsonClass(generateAdapter = true)
data class SseResponse(
    val id: String?,
    val choices: List<Choice>?
)

@JsonClass(generateAdapter = true)
data class Choice(
    val delta: Delta?
)

@JsonClass(generateAdapter = true)
data class Delta(
    val content: String?
)
```

### 4. Repository Implementation

Create a repository to handle the OkHttp connection and the `EventSourceListener`.

```kotlin
import android.util.Log
import com.squareup.moshi.Moshi
import com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.sse.EventSource
import okhttp3.sse.EventSourceListener
import okhttp3.sse.EventSources
import java.util.concurrent.TimeUnit

class LlmRepository {

    private val client = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(120, TimeUnit.SECONDS) // Keep high for LLM generation
        .build()

    private val moshi = Moshi.Builder().add(KotlinJsonAdapterFactory()).build()
    private val adapter = moshi.adapter(SseResponse::class.java)

    fun streamChat(
        message: String,
        onToken: (String) -> Unit,
        onComplete: () -> Unit,
        onError: (Throwable) -> Unit
    ): EventSource {

        val jsonRequest = """
            {
              "model": "llama3",
              "messages": [{"role": "user", "content": "${'$'}message"}],
              "stream": true
            }
        """.trimIndent()

        val requestBody = jsonRequest.toRequestBody("application/json".toMediaType())

        val request = Request.Builder()
            .url("http://10.0.2.2:8080/api/chat")
            .header("Accept", "text/event-stream")
            .post(requestBody)
            .build()

        val eventSourceListener = object : EventSourceListener() {
            override fun onEvent(eventSource: EventSource, id: String?, type: String?, data: String) {
                if (data == "[DONE]") {
                    return
                }
                try {
                    val response = adapter.fromJson(data)
                    val content = response?.choices?.firstOrNull()?.delta?.content
                    if (content != null) {
                        onToken(content)
                    }
                } catch (e: Exception) {
                    Log.e("LLM", "Error parsing SSE data", e)
                }
            }

            override fun onClosed(eventSource: EventSource) {
                onComplete()
            }

            override fun onFailure(eventSource: EventSource, t: Throwable?, response: Response?) {
                t?.let { onError(it) }
            }
        }

        return EventSources.createFactory(client).newEventSource(request, eventSourceListener)
    }
}
```

### 5. ViewModel Integration

Use a `ViewModel` to bridge the stream to the UI.

```kotlin
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.launch
import okhttp3.sse.EventSource

class ChatViewModel : ViewModel() {

    private val repository = LlmRepository()
    private var currentEventSource: EventSource? = null

    private val _chatText = MutableStateFlow("")
    val chatText: StateFlow<String> = _chatText

    fun sendMessage(message: String) {
        _chatText.value = "" // Clear previous response
        currentEventSource?.cancel() // Cancel ongoing stream if any

        currentEventSource = repository.streamChat(
            message = message,
            onToken = { token ->
                // Update UI state
                viewModelScope.launch(Dispatchers.Main) {
                    _chatText.value += token
                }
            },
            onComplete = {
                // Handle completion
            },
            onError = { error ->
                viewModelScope.launch(Dispatchers.Main) {
                    _chatText.value += "\n[Error: ${'$'}{error.message}]"
                }
            }
        )
    }

    override fun onCleared() {
        super.onCleared()
        currentEventSource?.cancel()
    }
}
```

### 6. Jetpack Compose UI

The UI simply observes the `StateFlow`. As new tokens arrive, Compose will automatically re-render the text, creating a smooth typing effect.

```kotlin
import androidx.compose.foundation.layout.*
import androidx.compose.material3.*
import androidx.compose.runtime.*
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp

@Composable
fun ChatScreen(viewModel: ChatViewModel) {
    val text by viewModel.chatText.collectAsState()
    var input by remember { mutableStateOf("") }

    Column(modifier = Modifier.padding(16.dp)) {
        // Output Text Area
        Text(
            text = text,
            modifier = Modifier.weight(1f)
        )

        // Input Area
        Row(modifier = Modifier.fillMaxWidth()) {
            OutlinedTextField(
                value = input,
                onValueChange = { input = it },
                modifier = Modifier.weight(1f),
                placeholder = { Text("Ask something...") }
            )
            Spacer(modifier = Modifier.width(8.dp))
            Button(onClick = { 
                viewModel.sendMessage(input)
                input = "" 
            }) {
                Text("Send")
            }
        }
    }
}
```

### 7. Sending Images (Vision Models)

If you are using a vision model (like `llava` or `moondream`), you can send an image along with your text prompt. The proxy backend supports passing an array of objects in the `content` field.

```kotlin
import android.util.Base64

// 1. Convert your Android Bitmap or byte[] to a Base64 string
val base64Image = Base64.encodeToString(imageByteArray, Base64.NO_WRAP)

// 2. Construct the JSON request for the proxy
val jsonRequest = """
    {
      "model": "llava",
      "messages": [
        {
          "role": "user",
          "content": [
            {
              "type": "text",
              "text": "What is happening in this picture?"
            },
            {
              "type": "image_url",
              "image_url": {
                "url": "data:image/jpeg;base64,${'$'}base64Image"
              }
            }
          ]
        }
      ],
      "stream": true
    }
""".trimIndent()
```
