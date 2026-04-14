package com.sprintsense.chatservice;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "messages")
public class Message {
    @Id
    private String id;
    private String channelId;
    private String senderId;
    private String senderName;
    private String content;
    private Long createdAt;
}
