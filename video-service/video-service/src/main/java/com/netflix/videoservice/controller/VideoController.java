package com.netflix.videoservice.controller;

import com.netflix.videoservice.service.VideoService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.IOException;

@RestController
@RequestMapping("/api/v1/videos")
@Slf4j
@RequiredArgsConstructor
public class VideoController {

    private final VideoService videoService;

    /*
   * Upload Video file for a movie
   * Accepts multipart file upload
   *
   * POST/api/v1/videos/upload/{movieId}
     */
    @PostMapping("/upload/{movieId}")
    public ResponseEntity<String> uploadMovie(
            @PathVariable String movieId,
            @RequestParam ("file") MultipartFile file) throws IOException {

        log.info("Upload request for movie: {}, Size: {} MB",
                movieId,file.getSize() / (1024*1024));//bytes to MegaBytes
        if(file.isEmpty()){
            return ResponseEntity.badRequest().body("File is empty");
        }
        String VideoKey = videoService.uploadVideo(movieId,file);

        return ResponseEntity.ok("Video Uploaded Successfully! Key" +
                VideoKey + "-> Encoding Started Automatically via Kafka ") ;
    }

}
