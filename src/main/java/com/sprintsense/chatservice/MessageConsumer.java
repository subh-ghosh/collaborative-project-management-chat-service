package com.sprintsense.chatservice;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Service
public class MessageConsumer {

    @Autowired
    private MessageRepository messageRepository;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private static final String CACHE_KEY_PREFIX = "chat:history:";
    private static final int MAX_HISTORY_SIZE = 50;

    @KafkaListener(topics = "CHAT_MESSAGE_SENT", groupId = "chat-persistence-group")
    public void consume(Message message) {
        try {
            System.out.println("📥 [Worker] Background persistence started for: " + message.getId());

            // 1. Persist to MongoDB
            messageRepository.save(message);

            // 2. Update Redis Cache (Recent History)
            String cacheKey = CACHE_KEY_PREFIX + message.getChannelId();

            // Push to the end of the list (history is sorted ASC)
            redisTemplate.opsForList().rightPush(cacheKey, message);

            // Keep only the last 50 messages
            redisTemplate.opsForList().trim(cacheKey, -MAX_HISTORY_SIZE, -1);

            // Set 24h expiration so abandoned chatrooms don't clog memory
            redisTemplate.expire(cacheKey, 24, TimeUnit.HOURS);

            System.out.println("✅ [Worker] Message saved to MongoDB & Redis: " + message.getId());
        } catch (Exception e) {
            System.err.println("❌ [Worker] Failed to process message " + message.getId() + ": " + e.getMessage());
        }
    }
}
