
DROP TABLE IF EXISTS Rating;
DROP TABLE IF EXISTS SongGenre;
DROP TABLE IF EXISTS Song;
DROP TABLE IF EXISTS Album;
DROP TABLE IF EXISTS User;
DROP TABLE IF EXISTS Genre;
DROP TABLE IF EXISTS Artist;

CREATE TABLE Artist (
    artist_id   INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    UNIQUE KEY uniq_artist_name (name)
) ENGINE = InnoDB;


CREATE TABLE Genre (
    genre_id    SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    UNIQUE KEY uniq_genre_name (name)
) ENGINE = InnoDB;


CREATE TABLE Album (
    album_id        INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    title           VARCHAR(200) NOT NULL,
    artist_id       INT UNSIGNED NOT NULL,
    release_date    DATE NOT NULL,
    genre_id        SMALLINT UNSIGNED NOT NULL,
    CONSTRAINT fk_album_artist
        FOREIGN KEY (artist_id) REFERENCES Artist(artist_id),
    CONSTRAINT fk_album_genre
        FOREIGN KEY (genre_id) REFERENCES Genre(genre_id),
    CONSTRAINT uniq_album_title_per_artist
        UNIQUE (artist_id, title)
) ENGINE = InnoDB;


CREATE TABLE Song (
    song_id      INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    title        VARCHAR(200) NOT NULL,
    artist_id    INT UNSIGNED NOT NULL,
    album_id     INT UNSIGNED NULL,
    release_date DATE NOT NULL,
    CONSTRAINT fk_song_artist
        FOREIGN KEY (artist_id) REFERENCES Artist(artist_id),
    CONSTRAINT fk_song_album
        FOREIGN KEY (album_id) REFERENCES Album(album_id),
    CONSTRAINT uniq_song_title_per_artist
        UNIQUE (artist_id, title)
) ENGINE = InnoDB;


CREATE TABLE User (
    user_id     INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    username    VARCHAR(50) NOT NULL,
    CONSTRAINT uniq_username UNIQUE (username)
) ENGINE = InnoDB;


CREATE TABLE Rating (
    user_id     INT UNSIGNED NOT NULL,
    song_id     INT UNSIGNED NOT NULL,
    rating      TINYINT NOT NULL,
    rating_date DATE NOT NULL,
    PRIMARY KEY (user_id, song_id),
    CONSTRAINT fk_rating_user
        FOREIGN KEY (user_id) REFERENCES User(user_id),
    CONSTRAINT fk_rating_song
        FOREIGN KEY (song_id) REFERENCES Song(song_id),
    CONSTRAINT chk_rating_value CHECK (rating BETWEEN 1 AND 5)
) ENGINE = InnoDB;


CREATE TABLE SongGenre (
    song_id  INT UNSIGNED NOT NULL,
    genre_id SMALLINT UNSIGNED NOT NULL,
    PRIMARY KEY (song_id, genre_id),
    CONSTRAINT fk_songgenre_song
        FOREIGN KEY (song_id)
        REFERENCES Song(song_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_songgenre_genre
        FOREIGN KEY (genre_id)
        REFERENCES Genre(genre_id)
        ON DELETE RESTRICT
) ENGINE = InnoDB;