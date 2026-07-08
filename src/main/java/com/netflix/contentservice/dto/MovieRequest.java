package com.netflix.contentservice.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.netflix.contentservice.model.Genre;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MovieRequest {

    @NotBlank(message = "Title is Required")
    private String  title;

    private String  description;

    @NotNull(message = "genre is required")
    private Genre genre;

    private String director;
    private String cast;
    private int releaseYear;

    private double rating;
    private String thumbnailUrl;
    private int durationMinutes;
}
