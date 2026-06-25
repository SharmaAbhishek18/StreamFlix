package com.netflix.contentservice.controller;

import com.netflix.contentservice.dto.MovieRequest;
import com.netflix.contentservice.dto.MovieResponse;
import com.netflix.contentservice.service.ContentService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import lombok.extern.slf4j.XSlf4j;
import model.Genre;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import javax.swing.text.AbstractDocument;
import java.util.List;

@RestController
@RequestMapping("/api/v1/movies")
@Slf4j
@RequiredArgsConstructor
public class ContentController {

    private final ContentService contentService;

    //add new movie to  catalog
    @PostMapping
    public ResponseEntity<MovieResponse>addMovie(
            @Valid @RequestBody MovieRequest movieRequest) {
            return ResponseEntity.status(HttpStatus.CREATED)
                    .body(contentService.addMovie(movieRequest));
    }

    @GetMapping
    public ResponseEntity <MovieResponse> getAllMovies() {
        return ResponseEntity.ok(contentService.getAllMovies());
    }

    //Get MOVIES by genre
    @GetMapping("/genre/{genre}")
    public ResponseEntity<List<MovieResponse>> getMoviesByGenre(
            @PathVariable Genre genre){
        return ResponseEntity.ok(contentService.getMoviesByGenre(genre));
    }

    //Get MOVIES by ID
    @GetMapping("/{movieId}")
    public ResponseEntity<MovieResponse> getMoviesById(
            @PathVariable String movieId){
        return ResponseEntity.ok(contentService.getMovieById(movieId));
    }

    @GetMapping("/search")
    public ResponseEntity<List<MovieResponse>>searchMovies(
            @RequestParam String title){
        return ResponseEntity.ok(contentService.searchMovies(title));
    }
}

