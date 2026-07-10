package com.netflix.encodingservice.event;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class VideoEncodedEvent {

    private String movieId;
    private String hlsUrl;
    private String masterPlaylistKey;
    private boolean success;
    private String errorMessage;

    public VideoEncodedEvent(
            String movieId,
            String hlsUrl,
            String masterPlaylistKey,
            boolean success,
            String errorMessage) {
        this.movieId = movieId;
        this.hlsUrl = hlsUrl;
        this.masterPlaylistKey = masterPlaylistKey;
        this.success = success;
        this.errorMessage = errorMessage;

    }
}