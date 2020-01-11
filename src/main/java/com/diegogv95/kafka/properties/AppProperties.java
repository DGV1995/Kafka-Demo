package com.diegogv95.kafka.properties;

import org.apache.kafka.common.serialization.StringDeserializer;

public interface AppProperties {
    // Kafka settings
    final String BOOTSTRAP_SERVERS = "localhost:9092";
    final String GROUP_ID = "users_group";
    final String ALL_USERS_TOPIC = "users";
    final String KEY_DESERIALIZER = StringDeserializer.class.getName();
    final String VALUE_DESERIALIZER = StringDeserializer.class.getName();
    final String OFFSET_RESET = "earliest";

    // Kafka Stream settings
    final String APP_ID_GT50 = "app-kafka-gt50";
    final String APP_ID_LT50 = "app-kafka-lt50";
    final String AGE_GREATHER_THAN_50_TOPIC = "users-gt-50";
    final String AGE_LESS_THAN_50_TOPIC = "users-lt-50";

    // Mongo settings
    final String MONGO_URI = "mongodb://localhost:27017";
    final String MONGO_DB = "data";
    final String MONGO_ALL_USERS_COLLECTION = "users";
    final String MONGO_GT50_USERS_COLLECTION = "users-gt50";
    final String MONGO_LT50_USERS_COLLECTION = "users-lt50";
}
