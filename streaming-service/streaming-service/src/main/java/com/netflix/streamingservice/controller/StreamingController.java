package com.netflix.streamingservice.controller;

import com.netflix.streamingservice.dto.StreamingResponse;
import com.netflix.streamingservice.service.StreamingService;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/stream")
@Slf4j
@RequiredArgsConstructor

public class StreamingController {

    private final StreamingService streamingService;
    private final RedisTemplate<String,String> redisTemplate;

    private static final String MASTER_PLAYLIST_KEY_PREFIX = "streaming:playlist:";
    /**
     * Get Streaming URL for a movie
     *  Returns Presigned HLS Master playlist URL
     *
     *  GET /api/v1/stream/{movieId}
     */
    @GetMapping("/{movieId}")
    public ResponseEntity<StreamingResponse>getStreamingUrl(
            @PathVariable String movieId){
        log.info("Streaming request for movie: {}",movieId);

        //get master playlist key from redis
        String playlistKey = redisTemplate.opsForValue()
                .get(MASTER_PLAYLIST_KEY_PREFIX + movieId);

        if(playlistKey == null){
        return ResponseEntity.notFound().build();
        }

        StreamingResponse response = streamingService.getStreamingUrl(movieId,playlistKey);
        return ResponseEntity.ok(response);


    }


}
