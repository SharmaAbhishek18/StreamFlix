package com.netflix.streamingservice.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class StreamingResponse {
    private String movieId;       //Presigned HLS MasterPlaylist URL
    private String streamingURL;  // Available qualities
    private String quality;
    private long expiredInMinutes; //URL expiry time
}
