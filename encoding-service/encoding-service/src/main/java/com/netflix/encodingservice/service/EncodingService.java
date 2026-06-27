package com.netflix.encodingservice.service;

import com.netflix.encodingservice.event.VideoEncodedEvent;
import com.netflix.encodingservice.event.VideoUploadedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Admin uploads Interstellar.mp4
 *                     ↓
 * Video Service
 *                     ↓
 * Upload raw video to S3
 *                     ↓
 * Publish Kafka Event
 *                     ↓
 * Encoding Service receives Event
 *                     ↓
 * Create local workspace
 *                     ↓
 * Download raw video from S3
 *                     ↓
 * FFmpeg creates 1080p,720p,480p,360p
 *                     ↓
 * Generate HLS playlists
 *                     ↓
 * Upload encoded files to S3
 *                     ↓
 * Publish "video.encoded" event
 *                     ↓
 * Movie ready for users to stream
 */

@Service
@Slf4j
@RequiredArgsConstructor //Automatically creates a constructor for all final fields.
public class EncodingService {

    //Used to communicate with AWS S3.
    private final S3Client s3Client;
    //Used to send events to Kafka.
    private final KafkaTemplate<String, VideoEncodedEvent> kafkaTemplate;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    @Value("${ffmpeg.path}")
    private String ffmpegPath;

    @Value("${encoding.base-path}")
    private String basePath;

    /**
     * After encoding:
     * kafkaTemplate.send(
     *         VIDEO_ENCODED_TOPIC,event);
     * This notifies other services:"Encoding finished."
     */


    private static final String VIDEO_ENCODED_TOPIC = "video.encoded";

    //Video Qualities to Encode
    //Format : resolution, bitrate,height
    //Bitrate -> How much data is Processed Per Minutes
    private static final List<int[]> VIDEO_QUALITIES = Arrays.asList(
            //new int[]{Width,Bitrate Height}
            new int[]{1920,5000,1080},  //1080p -> 5000k Bitrate
            new int[]{1280,2800,720},  // 720p -> 2800k Bitrate
            new int[]{854,1200,480},  // 480p -> 1200k Bitrate
            new int[]{640,800,360}    // 360p -> 800k Bitrate
    );

    /**
     * Main Encoding Pipeline
     * Steps:
     * 1. Download raw Video From s3.
     * 2. Encode to Multiple Qualities using FFmpeg.
     * 3. Generate HLS playlist(.m3u8) for each Quality.
     * 4. Create Master Playlist.
     * 5. Upload all Encoded Files Back to s3.
     * 6. Publish Video Encoded Event to Kafka
     * @param event
     */
    public void encodeVideo(VideoUploadedEvent event) {
        log.info("Starting Encoding Platform for movie: {}",event.getMovieId());

        //Create a Unique Path for movie -> Create Working Folder
        String jobPath = basePath + "/" + event.getMovieId();

        try{
            //Create Temp Directories
            // C:/videos/101/encoded
            Files.createDirectories(Paths.get(jobPath));

            Files.createDirectories(Paths.get(jobPath + "/encoded"));
            //C:/videos/101/encoded/
            //├── 1080p.m3u8
            //├── 720p.m3u8
            //├── 480p.m3u8
            //└── 360p.m3u8

            //Step 1 : Download raw video from s3
            String localVideoPath = jobPath + "/raw_video.mp4";
            downloadFromS3(event.getVideoKey(),localVideoPath);
            log.info("Raw video downloaded to {}", localVideoPath);

        // Step 2 & 3. Encode to Multiple Qualities + generate HLS
            for(int [] qualities : VIDEO_QUALITIES){
                 int width =   qualities[0];
                 int bitrate =  qualities[1];
                 int height = qualities[2];

                 // Create Quality Folder
                 String qualityDir = jobPath + "/encoded" + height + "p";
                 Files.createDirectories(Paths.get(qualityDir));

                 encodeToHLS(localVideoPath,qualityDir,width,height,bitrate);
                 log.info("Encoded {}p successfully ", height);
            }
        }
        catch (Exception e){

        }
    }

}
