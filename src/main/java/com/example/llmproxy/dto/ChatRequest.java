package com.example.llmproxy.dto;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
public class ChatRequest {
    private String model = "local-model"; // default, can be overridden by frontend
    private List<Message> messages;
    private boolean stream = true;
    private double temperature = 0.7;
}
