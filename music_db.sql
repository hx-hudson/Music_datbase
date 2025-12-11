-- 清理旧表，注意顺序：先删有外键依赖的子表，再删父表
DROP TABLE IF EXISTS Rating;
DROP TABLE IF EXISTS SongGenre;   -- 新增：SongGenre 依赖 Song，要先删
DROP TABLE IF EXISTS Song;
DROP TABLE IF EXISTS Album;
DROP TABLE IF EXISTS User;
DROP TABLE IF EXISTS Genre;
DROP TABLE IF EXISTS Artist;

-- 艺术家表：名字唯一
CREATE TABLE Artist (
    artist_id   INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(200) NOT NULL,
    UNIQUE KEY uniq_artist_name (name)
) ENGINE = InnoDB;

-- 流派表：预定义 genre 列表
CREATE TABLE Genre (
    genre_id    SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    UNIQUE KEY uniq_genre_name (name)
) ENGINE = InnoDB;

-- 专辑表：每张专辑一个 genre，(artist, title) 组合唯一
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

-- 歌曲表：不再直接存 genre_id，体裁信息全部放到 SongGenre 里
-- 属于专辑的歌必须在 SongGenre 中至少包含该专辑的 genre
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

-- 用户表：用户名唯一
CREATE TABLE User (
    user_id     INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    username    VARCHAR(50) NOT NULL,
    CONSTRAINT uniq_username UNIQUE (username)
) ENGINE = InnoDB;

-- 评分表：一个用户对一首歌最多一条评分
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

-- 歌曲与体裁的多对多关系：一首歌可以有多个 genre
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