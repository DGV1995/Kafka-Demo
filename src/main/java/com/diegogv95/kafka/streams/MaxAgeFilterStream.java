package com.diegogv95.kafka.streams;

import com.diegogv95.kafka.entity.User;
import com.diegogv95.kafka.properties.AppProperties;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import lombok.NoArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

@NoArgsConstructor
public class MaxAgeFilterStream {

    public static void main(String args[]) {
        new MaxAgeFilterStream().run();
    }

    public void run() {
        // Create props
        Properties props = createProperties();

        // Create topology
        Topology topology = createTopology();

        // Start the kafka streams service
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    public Properties createProperties() {
        Properties props = new Properties();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppProperties.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppProperties.APP_ID);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, AppProperties.OFFSET_RESET);

        return props;
    }

    public Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> inputStream = builder.stream(AppProperties.ALL_USERS_TOPIC);

        Gson gson = new Gson();

        KStream<String, String> filteredUsers = inputStream.filter((key, value) -> {
            User user = gson.fromJson(value, User.class);
            return user.getAge() > 50;
        });

        filteredUsers.to(AppProperties.AGE_GREATHER_THAN_50_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }
}
