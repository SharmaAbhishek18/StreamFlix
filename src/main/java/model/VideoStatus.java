package model;

/**
 * Tracks the video processing Lifecycle
 * FLOW:
 * PENDING->UPLOADED->ENCODING->ENCODED->READY->FAILED
 */
public enum VideoStatus {
    PENDING,//Movie added but not uploaded yet
    UPLOADED,//raw video uploaded to s3
    ENCODING,//FFmpeg is Encoding the video
    ENCODED,//ENCODING Complete
    READY,//HLS Playlist ready -> can be streamed
    FAILED//Encoding Failed
}
