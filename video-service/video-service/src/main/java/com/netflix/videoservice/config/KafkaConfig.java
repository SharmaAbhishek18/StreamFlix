 package com.netflix.videoservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {

    //Published When Video is Uploaded to S3

    /**
     * Topic used when a video is successfully uploaded to S3.
     * Producer  : Video Service
     * Consumer  : Encoding Service
     */
    @Bean // Tells Spring to create and manage the object.
    public NewTopic  videoUploadTopic(){

        return TopicBuilder

                // Name of the Kafka Topic
                .name("video.uploaded")
                // Number of partitions for parallel processing
                // More partitions => More consumers can process simultaneously
                .partitions(3)
                // Number of copies (backups) of each partition
                // For local development, 1 replica is sufficient
                .replicas(1)
                .build();
    }

    //Published When Encoding is Complete
    /**
     * Topic used after video encoding is completed.
     *
     * Producer : Encoding Service
     * Consumer : Notification/Catalog Service
     *
     * Example Event:
     * "Movie 101 encoded successfully"
     */
    @Bean
    public NewTopic  videoEncodedTopic(){
        return TopicBuilder.name("video.encoded")
                .partitions(3)
                .replicas(1)
                .build();
    }
}
