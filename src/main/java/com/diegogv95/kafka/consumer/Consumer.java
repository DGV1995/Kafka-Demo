package com.diegogv95.kafka.consumer;

import com.diegogv95.kafka.properties.AppProperties;
import com.mongodb.*;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@NoArgsConstructor
public class Consumer {
    Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    // Mongo settings
    final String database = "data";
    final String collection = "users";

    public static void main(String[] args) {
        new Consumer().run();
    }

    public void run() {
        // Create the consumer
        KafkaConsumer<String, String> consumer = createConsumer();

        while(true) {
            // Get the topic records
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                MongoClient mongo = new MongoClient(new MongoClientURI(AppProperties.MONGO_URI));
                DB db = mongo.getDB(AppProperties.MONGO_DB);
                DBCollection col = db.getCollection(AppProperties.MONGO_ALL_USERS_COLLECTION);

                col.insert(BasicDBObject.parse(record.value()));
                logger.info("User saved into MongoDB database: " + record.value());
            }
        }
    }

    public KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppProperties.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, AppProperties.KEY_DESERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AppProperties.VALUE_DESERIALIZER);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AppProperties.OFFSET_RESET);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, AppProperties.GROUP_ID);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(AppProperties.ALL_USERS_TOPIC));

        return consumer;
    }
}
