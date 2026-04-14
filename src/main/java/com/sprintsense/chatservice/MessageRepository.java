package com.sprintsense.chatservice;

import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.List;

public interface MessageRepository extends MongoRepository<Message, String> {
    List<Message> findByChannelIdOrderByCreatedAtAsc(String channelId);
}
