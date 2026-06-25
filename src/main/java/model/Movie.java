package model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.LocalDateTime;
import java.util.UUID;
@Table(name = "movies")
@Data
@NoArgsConstructor
@AllArgsConstructor
//@Builder
public class Movie {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private String id;
    @Column(nullable = false)
    private String title;

    @Column(length = 1000)
    private String description;

    @Enumerated(EnumType.STRING)
    private Genre genre;

    private String director;
    private String cast;
    private int releaseYear;

    private double rating;
    private String thumbnailUrl;
    private int durationMinutes;

    //S3 keys for the Video file
    private String videoKey;
    //HLS Master Playlist URL For Streaming
    private String hlsUrl;

    //Status Of Video Processing
    @Enumerated(EnumType.STRING)
    private VideoStatus videoStatus;

    @CreationTimestamp
    private LocalDateTime createdAt;

    @UpdateTimestamp
    private LocalDateTime updatedAt;

}
