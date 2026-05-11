@file:Suppress("unused")

package sample.chatfix

import android.content.ContentResolver
import android.net.Uri
import android.util.Base64
import android.util.Log
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import okhttp3.sse.EventSource
import okhttp3.sse.EventSourceListener
import okhttp3.sse.EventSources
import org.json.JSONArray
import org.json.JSONObject
import java.io.IOException
import java.util.UUID
import java.util.concurrent.TimeUnit

data class ChatMessage(
    val id: String,
    val text: String,
    val isFromUser: Boolean,
    val images: List<Uri> = emptyList()
)

data class ChatUiState(
    val isLoading: Boolean = false,
    val messages: List<ChatMessage> = emptyList(),
    val error: String? = null
)

sealed interface ChatEvent {
    data class SendMessage(
        val text: String,
        val images: List<Uri> = emptyList()
    ) : ChatEvent

    data object NewChat : ChatEvent
}

data class SseResponse(
    val choices: List<SseChoice>? = null
)

data class SseChoice(
    val delta: SseDelta? = null
)

data class SseDelta(
    val content: String? = null
)

class LlmRepository(
    private val contentResolver: ContentResolver,
    private val baseUrl: String = "http://10.0.2.2:8080",
    private val model: String = "llava"
) {

    private val client = OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(120, TimeUnit.SECONDS)
        .build()

    private val moshi = com.squareup.moshi.Moshi.Builder()
        .add(com.squareup.moshi.kotlin.reflect.KotlinJsonAdapterFactory())
        .build()
    private val adapter = moshi.adapter(SseResponse::class.java)

    fun streamChat(
        message: String,
        images: List<Uri>,
        onToken: (String) -> Unit,
        onComplete: () -> Unit,
        onError: (Throwable) -> Unit
    ): EventSource {

        val jsonRequest = buildJsonRequest(message = message, images = images)
        val requestBody = jsonRequest.toRequestBody("application/json".toMediaType())

        val request = Request.Builder()
            .url("$baseUrl/api/chat")
            .header("Accept", "text/event-stream")
            .post(requestBody)
            .build()

        Log.d("LlmRepository", "Initializing SSE connection to ${request.url}")

        val eventSourceListener = object : EventSourceListener() {
            override fun onOpen(eventSource: EventSource, response: Response) {
                Log.d(
                    "LlmRepository",
                    "SSE connection opened. Response code: ${response.code}"
                )
            }

            override fun onEvent(eventSource: EventSource, id: String?, type: String?, data: String) {
                if (data == "[DONE]") {
                    Log.d("LlmRepository", "Received [DONE] signal from SSE stream")
                    return
                }

                try {
                    val response = adapter.fromJson(data)
                    val content = response?.choices?.firstOrNull()?.delta?.content
                    if (!content.isNullOrEmpty()) {
                        onToken(content)
                    }
                } catch (error: Exception) {
                    Log.e("LlmRepository", "Error parsing SSE data: $data", error)
                }
            }

            override fun onClosed(eventSource: EventSource) {
                Log.d("LlmRepository", "SSE connection closed")
                onComplete()
            }

            override fun onFailure(eventSource: EventSource, t: Throwable?, response: Response?) {
                val failure = t ?: IOException("Unknown SSE failure. Response code: ${response?.code}")
                Log.e("LlmRepository", "SSE connection failed. Response code: ${response?.code}", failure)
                onError(failure)
            }
        }

        Log.d("LlmRepository", "Sending request")
        return EventSources.createFactory(client).newEventSource(request, eventSourceListener)
    }

    private fun buildJsonRequest(message: String, images: List<Uri>): String {
        val requestJson = JSONObject()
            .put("model", model)
            .put("stream", true)
            .put("messages", JSONArray().put(buildMessageJson(message, images)))

        return requestJson.toString()
    }

    private fun buildMessageJson(message: String, images: List<Uri>): JSONObject {
        val userMessage = JSONObject().put("role", "user")

        if (images.isEmpty()) {
            userMessage.put("content", message)
            return userMessage
        }

        val contentParts = JSONArray()
            .put(
                JSONObject()
                    .put("type", "text")
                    .put("text", message)
            )

        images.forEach { imageUri ->
            contentParts.put(
                JSONObject()
                    .put("type", "image_url")
                    .put(
                        "image_url",
                        JSONObject().put("url", imageUri.toDataUrl(contentResolver))
                    )
            )
        }

        userMessage.put("content", contentParts)
        return userMessage
    }
}

class ChatViewModel(
    private val repository: LlmRepository
) : ViewModel() {

    private var currentEventSource: EventSource? = null

    private val _uiState = MutableStateFlow(ChatUiState())
    val uiState: StateFlow<ChatUiState> = _uiState.asStateFlow()

    fun onEvent(event: ChatEvent) {
        when (event) {
            is ChatEvent.SendMessage -> sendMessage(event)
            ChatEvent.NewChat -> newChat()
        }
    }

    private fun sendMessage(event: ChatEvent.SendMessage) {
        val trimmedMessage = event.text.trim()
        if (trimmedMessage.isEmpty() && event.images.isEmpty()) {
            return
        }

        val userMessage = ChatMessage(
            id = UUID.randomUUID().toString(),
            text = trimmedMessage,
            isFromUser = true,
            images = event.images
        )

        val responseId = UUID.randomUUID().toString()
        val initialResponse = ChatMessage(
            id = responseId,
            text = "",
            isFromUser = false
        )

        _uiState.update { state ->
            state.copy(
                isLoading = true,
                error = null,
                messages = state.messages + userMessage + initialResponse
            )
        }

        currentEventSource?.cancel()
        currentEventSource = repository.streamChat(
            message = trimmedMessage,
            images = event.images,
            onToken = { token ->
                _uiState.update { state ->
                    state.copy(
                        messages = state.messages.map { message ->
                            if (message.id == responseId) {
                                message.copy(text = message.text + token)
                            } else {
                                message
                            }
                        }
                    )
                }
            },
            onComplete = {
                _uiState.update { state ->
                    state.copy(isLoading = false)
                }
            },
            onError = { error ->
                _uiState.update { state ->
                    state.copy(
                        isLoading = false,
                        error = error.message,
                        messages = state.messages.map { message ->
                            if (message.id == responseId) {
                                message.copy(text = message.text + "\n[Error: ${error.message ?: "Unknown error"}]")
                            } else {
                                message
                            }
                        }
                    )
                }
            }
        )
    }

    private fun newChat() {
        currentEventSource?.cancel()
        currentEventSource = null
        _uiState.update {
            ChatUiState()
        }
    }

    override fun onCleared() {
        currentEventSource?.cancel()
        currentEventSource = null
        super.onCleared()
    }
}

private fun Uri.toDataUrl(contentResolver: ContentResolver): String {
    val mimeType = contentResolver.getType(this) ?: "image/jpeg"
    val bytes = contentResolver.openInputStream(this)?.use { inputStream ->
        inputStream.readBytes()
    } ?: throw IOException("Unable to read image bytes from $this")

    val base64 = Base64.encodeToString(bytes, Base64.NO_WRAP)
    return "data:$mimeType;base64,$base64"
}
