package com.netflix.encodingservice.event;

/**
 * Consumed From Kafka topic : Video.uploaded.
 */
//@Data
//@NoArgsConstructor
//@AllArgsConstructor
public class VideoUploadedEvent {


    private String movieId;
    private String videoKey;
    private String bucketName;
    private String originalFilename;
    private Long  fileSizeBytes;
    public VideoUploadedEvent() {
    }

    public VideoUploadedEvent(String movieId, String videoKey,
                              String bucketName,
                              String originalFilename,
                              Long fileSizeBytes) {
        this.movieId = movieId;
        this.videoKey = videoKey;
        this.bucketName = bucketName;
        this.originalFilename = originalFilename;
        this.fileSizeBytes = fileSizeBytes;
    }

    public String getMovieId() {
        return movieId;
    }

    public String getVideoKey() {
        return videoKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getOriginalFilename() {
        return originalFilename;
    }

    public Long getFileSizeBytes() {
        return fileSizeBytes;
    }

    //new VideoUploadedEvent("raw/101/video.mp4","101","streamFlix-videos");


}
