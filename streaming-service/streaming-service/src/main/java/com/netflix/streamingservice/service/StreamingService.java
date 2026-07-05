package com.netflix.streamingservice.service;

import com.netflix.streamingservice.dto.StreamingResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

@Service
@RequiredArgsConstructor
@Slf4j
public class StreamingService {

    private final S3Presigner s3Presigner;
    private final RedisTemplate<String ,String> redisTemplate;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    @Value("${aws.s3.presigned-url-expiry}")
    private long presignedUrlExpiry;

    //Redis key for caching streaming URLs
    private final static String Streaming_URL_CACHE_PREFIX = "streaming:url:";

    /**
     * Get Streaming Url for a movie
     *
     *  * FLOW:
     *  * 1. Check Redis cache for existing presigned URL.
     *  * 2. If cached - return immediately.
     *  * 3. If not cached - generate new presigned URL from S3.
     *  * 4. Cache the URL in Redis.
     *  * 5. Return streaming URL.
     *  *
     *  * Why presigned URL?
     *  * - S3 bucket is private (locker room) - videos are not publicly accessible.
     *  * - Presigned URL gives temporary access (X minutes).
     *  * - Prevent unauthorized video downloads.
     *  */

    public StreamingResponse getStreamingUrl(String movieId,String playlistKey) {

    }


}
