package com.netflix.encodingservice.service;

import com.netflix.encodingservice.event.VideoEncodedEvent;
import com.netflix.encodingservice.event.VideoUploadedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

/**
 * Admin uploads Interstellar.mp4
 * ↓
 * Video Service
 * ↓
 * Upload raw video to S3
 * ↓
 * Publish Kafka Event
 * ↓
 * Encoding Service receives Event
 * ↓
 * Create local workspace
 * ↓
 * Download raw video from S3
 * ↓
 * FFmpeg creates 1080p,720p,480p,360p
 * ↓
 * Generate HLS playlists
 * ↓
 * Upload encoded files to S3
 * ↓
 * Publish "video.encoded" event
 * ↓
 * Movie ready for users to stream
 */

@Service
@Slf4j
@RequiredArgsConstructor //Automatically creates a constructor for all final fields.
public class EncodingService {

    /**
     * After encoding:
     * kafkaTemplate.send(
     * VIDEO_ENCODED_TOPIC,event);
     * This notifies other services:"Encoding finished."
     */


    private static final String VIDEO_ENCODED_TOPIC = "video.encoded";
    //Video Qualities to Encode
    //Format : resolution, bitrate,height
    //Bitrate -> How much data is Processed Per Minutes
    private static final List<int[]> VIDEO_QUALITIES = Arrays.asList(
            //new int[]{Width,Bitrate Height}
            new int[]{1920, 5000, 1080},  //1080p -> 5000k Bitrate
            new int[]{1280, 2800, 720},  // 720p -> 2800k Bitrate
            new int[]{854, 1200, 480},  // 480p -> 1200k Bitrate
            new int[]{640, 800, 360}    // 360p -> 800k Bitrate
    );
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
     * Main Encoding Pipeline
     * Steps:
     * 1. Download raw Video From s3.
     * 2. Encode to Multiple Qualities using FFmpeg.
     * 3. Generate HLS playlist(.m3u8) for each Quality.
     * 4. Create Master Playlist.
     * 5. Upload all Encoded Files Back to s3.
     * 6. Publish Video Encoded Event to Kafka
     *
     * @param event //h
     */

    public void encodeVideo(VideoUploadedEvent event) {
     //   log.info("Starting Encoding Platform for movie: {}", event.getMovieId());
        log.info("==============================================");
        log.info("ENCODING STARTED");
        log.info("Movie ID        : {}", event.getMovieId());
        log.info("Video Key       : {}", event.getVideoKey());
        log.info("Bucket          : {}", bucketName);
        log.info("Temp Directory  : {}", basePath);
        log.info("==============================================");
        //Create a Unique Path for movie -> Create Working Folder
        String jobPath = basePath + "/" + event.getMovieId();

        try {
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
            downloadFromS3(event.getVideoKey(), localVideoPath);
            //log.info("Raw video downloaded to {}", localVideoPath);
            File downloaded = new File(localVideoPath);

            log.info("----------------------------------------------");
            log.info("DOWNLOAD SUCCESS");
            log.info("Local Path : {}", localVideoPath);
            log.info("Exists     : {}", downloaded.exists());
            log.info("Size       : {} bytes", downloaded.length());
            log.info("----------------------------------------------");

            // Step 2 & 3. Encode to Multiple Qualities + generate HLS
            for (int[] qualities : VIDEO_QUALITIES) {
                int width = qualities[0];
                int bitrate = qualities[1];
                int height = qualities[2];

                // Create Quality Folder
                String qualityDir = jobPath + "/encoded/" + height + "p";
                Files.createDirectories(Paths.get(qualityDir));

                log.info("-----------------------------------------");
                log.info("Starting {}p Encoding", height);
                log.info("Resolution : {}x{}", width, height);
                log.info("Bitrate    : {} kbps", bitrate);
                log.info("Output Dir : {}", qualityDir);
                log.info("----------------------------------------");

                encodeToHLS(localVideoPath, qualityDir, width, height, bitrate);

                log.info("{}p Encoding Completed", height);
            }
            //Step 4 :Generate Master Playlist
            String masterPlaylistPath = jobPath + "/encoded/master.m3u8";
        //    generateMasterPlaylist(masterPlaylistPath);
        //    log.info("Master playlist Generated Successfully");
            generateMasterPlaylist(masterPlaylistPath);

            File master = new File(masterPlaylistPath);

            log.info("======================================");
            log.info("MASTER PLAYLIST GENERATED");
            log.info("Exists : {}", master.exists());
            log.info("Path   : {}", masterPlaylistPath);
            log.info("======================================");

            //Step - 5 : Upload all resources file to s3
            String encodedPrefix = "encoded/" + event.getMovieId() + "/";
            log.info("Uploading encoded files to S3...");
            uploadedEncodedFilesToS3(jobPath + "/encoded", encodedPrefix);
            log.info("All Encoded files Uploaded to s3 successfully ");

            //Step 6: Publish video Encoded Event
            String masterPlaylistKey = encodedPrefix + "master.m3u8";
            String hlsUrl = "https://" + bucketName + ".s3.amazonaws.com/" + masterPlaylistKey;

            VideoEncodedEvent encodedEvent = new VideoEncodedEvent(
                    event.getMovieId(),
                    hlsUrl,
                    masterPlaylistKey,
                    true,
                    null
            );
            log.info("=========================================");
            log.info("VIDEO ENCODED EVENT");
            log.info("Movie ID        : {}", event.getMovieId());
            log.info("Master Playlist : {}", masterPlaylistKey);
            log.info("HLS URL         : {}", hlsUrl);
            log.info("Kafka Topic     : {}", VIDEO_ENCODED_TOPIC);
            log.info("=========================================");
           // kafkaTemplate.send(VIDEO_ENCODED_TOPIC, event.getMovieId(), encodedEvent);
            kafkaTemplate.send(
                    VIDEO_ENCODED_TOPIC,
                    event.getMovieId(),
                    encodedEvent
            );

            log.info("Kafka Event Published Successfully.");
            log.info("VideoEncodedEvent Published for movie {}", event.getMovieId());

        } catch (Exception e) {
            //log.error("Encoding failed for movie : {} - {}", event.getMovieId(), e.getMessage());
            log.error("====================================");
            log.error("ENCODING FAILED");
            log.error("Movie ID : {}", event.getMovieId());
            log.error("Reason   : {}", e.getMessage(), e);
            log.error("====================================");
            //Publish Failure Event
            VideoEncodedEvent failureEvent = new VideoEncodedEvent(
                    event.getMovieId(),
                    null,
                    null,
                    false,
                    e.getMessage()
            );
            kafkaTemplate.send(VIDEO_ENCODED_TOPIC, event.getMovieId(), failureEvent);
        } finally {
            //Clean up temp files
            cleanupTempFiles(jobPath);
        }
    }

    /**
     * Download file from s3 to local path
     */
    private void downloadFromS3(String s3Key, String localPath) {
        //PutObjectRequest uploads a file to S3, while GetObjectRequest retrieves a file from Amazon S3 bucket.
        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(s3Key)
                .build();
        s3Client.getObject(getObjectRequest, Paths.get(localPath));
    }

    /**
     * Encode Video HLS Using FFmpeg
     * - Multiple .ts segment files(10 seconds each)
     * - A .m3u8 playlist file for this quality
     *
     * @param inputPath
     * @param outputDir
     * @param width
     * @param height
     * @param bitrate
     * @throws IOException
     * @throws InterruptedException
     */ // HLS -> HTTP Live Streaming
    private void encodeToHLS(
            String inputPath,
            String outputDir,
            int width,
            int height,
            int bitrate) throws IOException, InterruptedException {
        String playlistPath = outputDir + "/playlist.m3u8";
        String segmentPattern = outputDir + "/segment_%03d.ts";

        //FFmpeg Command for HLS Encoding
        List<String> command = Arrays.asList(
                ffmpegPath,
                "-i", inputPath,                        //Input file
                "-vf", "scale=" + width + ":" + height, //Scale to Resolution-> vf = Video Filter
                "-c:v", "libx264",                      //Video codec
                "-b:v", bitrate + "K",                  //video bitrate
                "-c:a", "aac",                          //Audio Coded
                "-b:a", "128k",                         // Audio Bitrate
                "-hls_time", "10",                      // 10 sec segments
                "-hls_list_size", "0",                  // keep all Segments
                "-hls_segment_filename", segmentPattern,//segment naming
                "-f", "hls",                            // Output Format HLS
                playlistPath                            //OutPut Playlist
        );
        log.info("========================================");
        log.info("Executing FFmpeg");
        log.info(String.join(" ", command));
        log.info("========================================");
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);
        processBuilder.inheritIO();
        Process process = processBuilder.start();
//        int exitCode = process.waitFor();
//        if (exitCode != 0) {
//            throw new RuntimeException("FFmpeg encoding failed with exit code " + exitCode);
//        }
        int exitCode = process.waitFor();

        log.info("FFmpeg Exit Code : {}", exitCode);

        if (exitCode != 0) {
            throw new RuntimeException("FFmpeg failed with exit code " + exitCode);
        }

       // log.info("FFmpeg Finished Successfully");
        log.info("FFmpeg Finished Successfully for {}p", height);
    }

    /**
     * Generate Master HLS Playlist that references all Quality playlist.
     * This is the file the video player downloads first;
     *
     * @param masterPlaylistPath
     * @throws IOException
     */
    private void generateMasterPlaylist(String masterPlaylistPath) throws IOException {
        StringBuilder master = new StringBuilder();
        master.append("#EXTM3U\n");
        master.append("#EXT-X-VERSION:3\n\n");

        //Add each quality to master Playlist
        int[][] qualities = {{1920, 5000, 1080}, {1280, 2800, 720},
                {854, 1200, 480}, {640, 800, 360}};
        for (int[] q : qualities) {
            int width = q[0];
            int bitrate = q[1];
            int height = q[2];

            master.append("#EXT-X-STREAM-INF:BANDWIDTH=")
                    .append(bitrate * 1000)
                    .append(", RESOLUTION=").append(width).append("x").append(height)
                    .append(",CODECS=\"avc1.42e01e,mp4a.40.2\"\n");
            master.append(height).append("p/playlist.m3u8\n\n");
        }
        Files.writeString(Paths.get(masterPlaylistPath), master.toString());
    }

    private void uploadedEncodedFilesToS3(String localDir, String s3Prefix) throws IOException {
        File directory = new File(localDir);
        uploadDirectoryToS3(directory, localDir, s3Prefix);
    }

    /**
     * Upload all encoded files from local back to s3
     *
     * @param dir
     * @param baseDir
     * @param s3Prefix
     */
    private void uploadDirectoryToS3(File dir, String baseDir, String s3Prefix) throws IOException {

        File[] files = dir.listFiles();

        if (files == null) {
            log.warn("No files found in directory: {}", dir.getAbsolutePath());
            return;
        }

        for (File file : files) {

            if (file.isDirectory()) {
                uploadDirectoryToS3(file, baseDir, s3Prefix);
                continue;
            }

            String relativePath = file.getAbsolutePath()
                    .substring(baseDir.length() + 1)
                    .replace("\\", "/");

            String s3Key = s3Prefix + relativePath;

            String contentType = file.getName().endsWith(".m3u8")
                    ? "application/x-mpegURL"
                    : "video/MP2T";

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .contentType(contentType)
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromFile(file));

            log.info("================================== ======");
            log.info("UPLOAD SUCCESS");
            log.info("Local File  : {}", file.getAbsolutePath());
            log.info("S3 Key      : {}", s3Key);
            log.info("ContentType : {}", contentType);
            log.info("==================== ====================");
        }
    }

    /**
     * Clean Up temp files after Encoding
     */
    private void cleanupTempFiles(String jobPath) {
        try {
            Path dirPath = Paths.get(jobPath);
            if (Files.exists(dirPath)) {
                Files.walk(dirPath)
                        .sorted(java.util.Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
                //log.info("Cleaned up temp files for job :{}", jobPath);
                log.info("Temporary workspace deleted : {}", jobPath);
            }
        } catch (IOException e) {
            //log.warn("Failed to clean up temp files for job :{}", e.getMessage());
            log.warn("Failed to delete temp folder : {}", jobPath, e);
        }
    }
}
