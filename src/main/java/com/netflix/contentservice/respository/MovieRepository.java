package com.netflix.contentservice.respository;

import com.netflix.contentservice.dto.MovieResponse;
import model.Genre;
import model.Movie;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
@Repository
public interface MovieRepository extends JpaRepository<Movie, String> {


    List<Movie> findByGenre(Genre genre);

    List<Movie> findByTitleContainingIgnoreCase(String title);
}
