package com.netflix.contentservice.dto;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import model.Genre;
import model.VideoStatus;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class MovieResponse {

    private String id;
    private String title;
    private String description;
    private Genre genre;
    private String director;
    private String cast;
    private int releaseYear;
    private double rating;
    private String thumbnailUrl;
    private int durationMinutes;
    private String videoKey;
    private String hlsUrl;
    private LocalDateTime createdAt;

    //Status Of Video Processing
    @Enumerated(EnumType.STRING)
    private VideoStatus videoStatus;
}
