package com.netflix.streamingservice.event;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
/**
 * Consumed from Kafka topic: video.encoded
 * Published by Encoding Service after FFmpeg processing.
 */
public class VideoEncodedEvent {
    private String movieId;
    private String hlsUrl; //Master Playlist URL for streaming
    private String masterPlaylistKey;//S3 key if master.m3u8
    private boolean success;
    private  String errorMessage;// If Encoding failed
}
