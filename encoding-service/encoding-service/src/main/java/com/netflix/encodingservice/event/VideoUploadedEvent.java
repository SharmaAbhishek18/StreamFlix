package com.netflix.encodingservice.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Consumed From Kafka topic : Video.uploaded.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class VideoUploadedEvent {


    private String movieId;
    private String videoKey;
    private String bucketName;
    private String originalFilename;
    private String fileSizeBytes;

    //new VideoUploadedEvent("raw/101/video.mp4","101","streamFlix-videos");


}
