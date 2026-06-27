package com.netflix.videoservice.service;

import com.netflix.videoservice.event.VideoUploadedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import java.io.IOException;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class VideoService {

    private final S3Client s3Client;

    private final KafkaTemplate<String, VideoUploadedEvent> kafkaTemplate;

    @Value("${aws.s3.bucket-name}")
    private String bucketName;

    private static final String VIDEO_UPLOADED_TOPIC = "video.uploaded";

    /**
     * Upload Video to AWS S3 and publish VideoUploadedEvent to Kafka
     * <p>
     * FLOW
     * 1. Receive Multipart video file
     * 2. Generate Unique s3 key
     * 3. Upload to s3
     * 4. Publish VideoUploadedEvent to Kafka
     * 5. Encoding service picks up and start FFMPeg
     */
    public String uploadVideo(String movieId, MultipartFile file) throws IOException {
        log.info("Start Uploading video for movie: {} file: {}", movieId, file.getOriginalFilename());

        //Generate Unique S3 key for raw video
        //Format : raw/movieId/uuid_filename

        String videoKey = "raw/" + movieId + "/" + UUID.randomUUID() + "_" + file.getOriginalFilename();

        PutObjectRequest putObjectRequest = PutObjectRequest
                .builder()
                .bucket(bucketName)
                .key(videoKey)
                .contentType(file.getContentType())
                .contentLength(file.getSize())
                .build();
        s3Client.putObject(putObjectRequest,
                RequestBody.fromInputStream(file.getInputStream(), file.getSize()));

        log.info("Video Uploaded to S3 successfully Key: {} ", videoKey);

        //Publish Event to Kafka
        //Encoding Service Will Consume this and Start FFMpeg processing

        VideoUploadedEvent event = new VideoUploadedEvent(
                videoKey,
                movieId,
                bucketName,
                file.getOriginalFilename(),
                file.getSize()
        );
        kafkaTemplate.send(VIDEO_UPLOADED_TOPIC, movieId, event);
        log.info("VideoUploaded to S3 successfully Key: {} ", movieId);

        return videoKey;

    }
}
