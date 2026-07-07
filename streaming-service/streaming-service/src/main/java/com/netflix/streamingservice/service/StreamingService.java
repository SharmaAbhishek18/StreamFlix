package com.netflix.streamingservice.service;

import com.netflix.streamingservice.dto.StreamingResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * User clicks Play
 *         │
 *         ▼
 * Controller
 *         │
 *         ▼
 * StreamingService
 *         │
 *         ▼
 * Redis
 *         │
 *    Cache Hit?
 *    ┌───────────────┐
 *    │               │
 *   Yes             No
 *    │               │
 * Return URL         │
 *                    ▼
 *              Generate Presigned URL
 *                    │
 *                    ▼
 *              Save in Redis (55 min)
 *                    │
 *                    ▼
 *              Return URL to User
 */

@Service
@RequiredArgsConstructor
@Slf4j
public class StreamingService {

    private S3Client  s3Client;
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
        log.info("Getting streaming URL for movie : {}",movieId);

        String cacheKey = Streaming_URL_CACHE_PREFIX + movieId ;

        //Check redis cache first
        String cachedUrl = redisTemplate.opsForValue().get(cacheKey);

        if (cachedUrl != null) {
            log.info("Found streaming URL for movie : {}",movieId);
            return new StreamingResponse(movieId,cachedUrl, //Cache Hit
                    "1080p,720p,480p,360p",presignedUrlExpiry);
        }
        //Generate Presigned URL from S3
        log.info("Generating new presigned URL for movie : {}",movieId);

        String presignedUrl = generatePresignedUrl(playlistKey);

        //  Cache in redis for 55 minutes
        // (5 minutes less that actual expiry to avoid edge cases)
        redisTemplate.opsForValue().set(
                cacheKey, // streaming:url:101
                presignedUrl,
                55,
                TimeUnit.MINUTES);
        log.info("Streaming URL generated and cached for movie : {}",movieId );

        return new StreamingResponse(
                movieId,
                presignedUrl,
                "1080p,720p,480p,360p",
                presignedUrlExpiry
        );
    }
    /**
     * THIS is the key method that makes everything SECURE.
     * @param movieId
     * @param playlistPath
     * @return
     */
    public String getSignedPlaylist(String movieId,String playlistPath) {

        //Get base path for this playlist
        String basePath = playlistPath.substring(0, playlistPath.lastIndexOf('/') + 1);

        //Read the m3u8 content from s3
        String m3u8Content = readFromS3(playlistPath);

        //Rewrite each line that is a segment or playlist reference
        String signedContent = rewriteM3u8SignedUrls(
                m3u8Content,basePath);

        return signedContent;
    }
    private String rewriteM3u8SignedUrls(String m3u8Content, String basePath) {

        StringBuilder reWritten = new StringBuilder();

        for(String line : m3u8Content.split("\n")) {
            String trimmed = line.trim();

            // Skip empty lines and comments
            if(trimmed.isEmpty() || trimmed.startsWith("#")) {
                reWritten.append(line).append("\n");
                continue;
            }
            // This is a segment or playlist reference
            // Build full S3 key and Sign it
            String fullKey = basePath + trimmed;
            String signedUrl = generatePresignedUrl(fullKey);
            reWritten.append(signedUrl).append("\n");
        }
        return reWritten.toString();
    }
    /**
     * Read file content from S3.
     */
    private String readFromS3(String s3Key) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();

        ResponseInputStream<GetObjectResponse> response =
                s3Client.getObject(request);

        return new BufferedReader(new InputStreamReader(response))
                .lines()
                .collect(Collectors.joining("\n"));
    }


    // Cache Miss -> Now AWS must generate a new URL.
    /**
     * Generate a Presigned Url for S3 Object
     * URL expired after Configured time
     */
    private String generatePresignedUrl(String key) {

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName) //netflix-movies
                .key(key) //I want to access the object movies/avatar/master.m3u8 from the netflix-movies bucket.
                .build(); //Creates the final immutable request object.

        GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                .signatureDuration(Duration.ofMinutes(presignedUrlExpiry))
                .getObjectRequest(getObjectRequest)
                .build();
        return s3Presigner.presignGetObject(presignRequest)
                .url()
                .toString();
    }
    /**
     * Invalidate Cache streaming URL
     * Called when video is re-encoded or updated
     */

    public void invalidateCache(String movieId) {
        String cacheKey = Streaming_URL_CACHE_PREFIX + movieId;
        redisTemplate.delete(cacheKey);
        log.info("Streaming URL cache invalidated for movie : {}",movieId);
    }


}
