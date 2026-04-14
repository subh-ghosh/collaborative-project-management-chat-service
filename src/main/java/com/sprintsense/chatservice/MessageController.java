package com.sprintsense.chatservice;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import java.util.UUID;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/chats")
@CrossOrigin(origins = "*")
public class MessageController {

    @Autowired
    private MessageRepository messageRepository;

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private static final String CACHE_KEY_PREFIX = "chat:history:";

    @GetMapping("/{sprintId}/messages")
    public List<Message> getMessages(@PathVariable String sprintId) {
        String cacheKey = CACHE_KEY_PREFIX + sprintId;

        // 1. Try to fetch from Redis Cache (Last 50 messages)
        try {
            List<Object> cachedMessages = redisTemplate.opsForList().range(cacheKey, 0, -1);
            if (cachedMessages != null && !cachedMessages.isEmpty()) {
                System.out.println("⚡ [API] Cache Hit: Returning " + cachedMessages.size() + " messages from Redis for "
                        + sprintId);
                return cachedMessages.stream()
                        .map(obj -> (Message) obj)
                        .collect(Collectors.toList());
            }
        } catch (Exception e) {
            System.err.println("⚠️ [API] Redis error, falling back to MongoDB: " + e.getMessage());
        }

        // 2. Cache Miss: Fetch from MongoDB
        System.out.println("💾 [API] Cache Miss: Fetching from MongoDB for " + sprintId);
        List<Message> messages = messageRepository.findByChannelIdOrderByCreatedAtAsc(sprintId);

        // 3. Populate Redis asynchronously (Optional, but good for next user)
        if (!messages.isEmpty()) {
            final List<Message> recentMessages = messages.size() > 50
                    ? messages.subList(messages.size() - 50, messages.size())
                    : messages;

            try {
                redisTemplate.delete(cacheKey);
                redisTemplate.opsForList().rightPushAll(cacheKey, recentMessages.toArray());
                redisTemplate.expire(cacheKey, 24, TimeUnit.HOURS);
            } catch (Exception e) {
                System.err.println("⚠️ [API] Failed to populate Redis: " + e.getMessage());
            }
        }

        return messages;
    }

    @PostMapping("/{sprintId}/messages")
    public Message saveMessage(@PathVariable String sprintId, @RequestBody Message message) {
        if (message.getId() == null || !message.getId().startsWith("msg_")) {
            message.setId("msg_" + UUID.randomUUID().toString().replace("-", ""));
        }
        message.setChannelId(sprintId);

        if (message.getCreatedAt() == null) {
            message.setCreatedAt(System.currentTimeMillis());
        }

        // Publish to Kafka (Worker will handle persistence and Redis update)
        try {
            kafkaTemplate.send("CHAT_MESSAGE_SENT", message);
            System.out.println("🚀 [API] Message published to Kafka: " + message.getId());
        } catch (Exception e) {
            System.err.println("❌ [API] Kafka publish failed: " + e.getMessage());
            throw new RuntimeException("Real-time system unavailable");
        }

        return message;
    }
}
