from typing import Tuple, List, Set


# ----------------------
# Helper functions
# ----------------------

def _get_or_create_artist(mydb, artist_name: str) -> int:
    """
    返回 artist_id，如果不存在则创建。

    假设 Artist 表结构为：
        Artist(artist_id PK AUTO_INCREMENT, name UNIQUE NOT NULL)
    （没有 is_group 列）
    """
    cur = mydb.cursor()
    cur.execute("SELECT artist_id FROM Artist WHERE name = %s", (artist_name,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("INSERT INTO Artist (name) VALUES (%s)", (artist_name,))
    mydb.commit()
    return cur.lastrowid


def _get_artist_id(mydb, artist_name: str):
    """返回 artist_id，不存在则返回 None。"""
    cur = mydb.cursor()
    cur.execute("SELECT artist_id FROM Artist WHERE name = %s", (artist_name,))
    row = cur.fetchone()
    return row[0] if row else None


def _get_or_create_genre(mydb, genre_name: str) -> int:
    """返回 genre_id，如果不存在则创建。"""
    cur = mydb.cursor()
    cur.execute("SELECT genre_id FROM Genre WHERE name = %s", (genre_name,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("INSERT INTO Genre (name) VALUES (%s)", (genre_name,))
    mydb.commit()
    return cur.lastrowid


def _get_song_id_by_artist_and_title(mydb, artist_id: int, title: str):
    """通过 artist_id + title 查 song_id，不存在返回 None。"""
    cur = mydb.cursor()
    cur.execute(
        "SELECT song_id FROM Song WHERE artist_id = %s AND title = %s",
        (artist_id, title),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _get_user_id(mydb, username: str):
    """返回 user_id，不存在返回 None。"""
    cur = mydb.cursor()
    cur.execute("SELECT user_id FROM User WHERE username = %s", (username,))
    row = cur.fetchone()
    return row[0] if row else None


# ----------------------
# 1. 清空数据库
# ----------------------

def clear_database(mydb):
    """
    Deletes all the rows from all the tables of the database.
    If a table has a foreign key to a parent table, it is deleted before
    deleting the parent table, otherwise the database system will throw an error.

    Args:
        mydb: database connection
    """
    cur = mydb.cursor()

    # 有外键依赖的子表先删，再删父表
    # Rating -> Song / User
    # SongGenre -> Song / Genre
    # Song -> Album / Artist
    # Album -> Artist / Genre
    for table in ["Rating", "SongGenre", "Song", "Album", "User", "Genre", "Artist"]:
        cur.execute(f"DELETE FROM {table}")

    mydb.commit()


# ----------------------
# 2. 加载单曲
# ----------------------

def load_single_songs(
    mydb,
    single_songs: List[Tuple[str, Tuple[str, ...], str, str]],
) -> Set[Tuple[str, str]]:
    """
    Add single songs to the database.

    Args:
        mydb: database connection

        single_songs: List of single songs to add. Each single song is a tuple of the form:
              (song title, genre names, artist name, release date)
        Genre names is a tuple since a song could belong to multiple genres.
        Release date is of the form yyyy-mm-dd

    Returns:
        Set[Tuple[str,str]]: set of (song,artist) for combinations that already exist
        in the database and were not added (rejected).
        Set is empty if there are no rejects.
    """
    rejected: Set[Tuple[str, str]] = set()
    cur = mydb.cursor()

    for title, genre_names, artist_name, release_date in single_songs:
        # 单曲：必须至少有一个体裁，否则 reject
        if not genre_names:
            rejected.add((title, artist_name))
            continue

        # 创建 / 获取 artist
        artist_id = _get_or_create_artist(mydb, artist_name)

        # 检查 (artist, title) 是否已经存在
        cur.execute(
            "SELECT song_id FROM Song WHERE artist_id = %s AND title = %s",
            (artist_id, title),
        )
        if cur.fetchone():
            rejected.add((title, artist_name))
            continue

        # 插入单曲（album_id 为 NULL；策略 B 下 Song 没有 genre_id）
        cur.execute(
            """
            INSERT INTO Song (title, artist_id, album_id, release_date)
            VALUES (%s, %s, NULL, %s)
            """,
            (title, artist_id, release_date),
        )
        song_id = cur.lastrowid

        # 对这首单曲的所有体裁插入 SongGenre（可以多体裁）
        # 用 set 去重，避免传入重复 genre 名导致违反 (song_id, genre_id) 主键
        unique_genres = set(genre_names)
        for genre_name in unique_genres:
            genre_id = _get_or_create_genre(mydb, genre_name)
            cur.execute(
                "INSERT INTO SongGenre (song_id, genre_id) VALUES (%s, %s)",
                (song_id, genre_id),
            )

    mydb.commit()
    return rejected

# ----------------------
# 3. 最“高产”的个人歌手（按单曲数）
# ----------------------

def get_most_prolific_individual_artists(
    mydb,
    n: int,
    year_range: Tuple[int, int],
) -> List[Tuple[str, int]]:
    """
    Get the top n most prolific individual artists by number of singles released in a year range.
    Break ties by alphabetical order of artist name.

    由于 Artist 表不再有 is_group，这里把所有 artist 都当作 individual 处理。

    Args:
        mydb: database connection
        n: how many to get
        year_range: tuple, e.g. (2015,2020)

    Returns:
        List[Tuple[str,int]]: list of (artist name, number of songs) tuples.
    """
    start_year, end_year = year_range
    cur = mydb.cursor()

    # 只统计单曲：album_id IS NULL
    cur.execute(
        """
        SELECT a.name, COUNT(*) AS num_singles
        FROM Song AS s
        JOIN Artist AS a ON s.artist_id = a.artist_id
        WHERE s.album_id IS NULL
          AND YEAR(s.release_date) BETWEEN %s AND %s
        GROUP BY a.artist_id, a.name
        ORDER BY num_singles DESC, a.name ASC
        LIMIT %s
        """,
        (start_year, end_year, n),
    )

    rows = cur.fetchall()
    return [(name, count) for (name, count) in rows]


# ----------------------
# 4. 最后一首单曲在某年的歌手
# ----------------------

def get_artists_last_single_in_year(mydb, year: int) -> Set[str]:
    """
    Get all artists who released their last single in the given year.

    Args:
        mydb: database connection
        year: year of last release

    Returns:
        Set[str]: set of artist names
    """
    cur = mydb.cursor()

    # 对每个 artist，找出所有单曲 release_date 的最大值，再过滤年份
    cur.execute(
        """
        SELECT a.name, MAX(s.release_date) AS last_date
        FROM Song AS s
        JOIN Artist AS a ON s.artist_id = a.artist_id
        WHERE s.album_id IS NULL
        GROUP BY a.artist_id, a.name
        HAVING YEAR(last_date) = %s
        """,
        (year,),
    )

    return {name for (name, _last_date) in cur.fetchall()}


# ----------------------
# 5. 加载专辑
# ----------------------

def load_albums(
    mydb,
    albums: List[Tuple[str, str, str, str, List[str]]],
) -> Set[Tuple[str, str]]:
    """
    Add albums to the database.

    Args:
        mydb: database connection

        albums: List of albums to add. Each album is a tuple of the form:
              (album title, genre, artist name, release date, list of song titles)
        Release date is of the form yyyy-mm-dd

    Returns:
        Set[Tuple[str,str]]: set of (album, artist) combinations that were not added (rejected)
        because the artist already has an album of the same title.
    """
    rejected: Set[Tuple[str, str]] = set()
    cur = mydb.cursor()

    for album_title, album_genre, artist_name, release_date, song_titles in albums:
        # 创建 / 获取 artist
        artist_id = _get_or_create_artist(mydb, artist_name)

        # 专辑体裁（Album 仍然有 genre_id）
        album_genre_id = _get_or_create_genre(mydb, album_genre)

        # 检查该 artist 是否已有同名专辑
        cur.execute(
            "SELECT album_id FROM Album WHERE artist_id = %s AND title = %s",
            (artist_id, album_title),
        )
        row = cur.fetchone()
        if row:
            rejected.add((album_title, artist_name))
            continue

        # 插入专辑
        cur.execute(
            """
            INSERT INTO Album (title, artist_id, release_date, genre_id)
            VALUES (%s, %s, %s, %s)
            """,
            (album_title, artist_id, release_date, album_genre_id),
        )
        album_id = cur.lastrowid

        # 专辑里的所有歌曲：体裁必须与专辑一致
        for song_title in song_titles:
            # 避免冲突：同一 artist + title 已有则跳过该曲目（不拒绝整张专辑）
            cur.execute(
                "SELECT song_id FROM Song WHERE artist_id = %s AND title = %s",
                (artist_id, song_title),
            )
            if cur.fetchone():
                continue

            # 插入 Song（无 genre_id）
            cur.execute(
                """
                INSERT INTO Song (title, artist_id, album_id, release_date)
                VALUES (%s, %s, %s, %s)
                """,
                (song_title, artist_id, album_id, release_date),
            )
            song_id = cur.lastrowid

            # 在 SongGenre 中插入一条，体裁 = 专辑体裁
            cur.execute(
                "INSERT INTO SongGenre (song_id, genre_id) VALUES (%s, %s)",
                (song_id, album_genre_id),
            )

    mydb.commit()
    return rejected


# ----------------------
# 6. 歌曲最多的体裁
# ----------------------

def get_top_song_genres(mydb, n: int) -> List[Tuple[str, int]]:
    """
    Get n genres that are most represented in terms of number of songs in that genre.
    Songs include singles as well as songs in albums.

    在策略 B 下，歌曲体裁信息存储在 SongGenre，因此需要通过 SongGenre 关联 Genre。

    Args:
        mydb: database connection
        n: number of genres

    Returns:
        List[Tuple[str,int]]: (genre,number_of_songs)，按歌数从多到少，tie 时按 genre 名字字典序。
    """
    cur = mydb.cursor()
    cur.execute(
        """
        SELECT g.name, COUNT(*) AS num_songs
        FROM Song AS s
        JOIN SongGenre AS sg ON s.song_id = sg.song_id
        JOIN Genre AS g      ON sg.genre_id = g.genre_id
        GROUP BY g.genre_id, g.name
        ORDER BY num_songs DESC, g.name ASC
        LIMIT %s
        """,
        (n,),
    )
    rows = cur.fetchall()
    return [(name, count) for (name, count) in rows]

# ----------------------
# 7. 既有专辑又有单曲的 artist
# ----------------------

def get_album_and_single_artists(mydb) -> Set[str]:
    """
    Get artists who have released albums as well as singles.

    Args:
        mydb: database connection

    Returns:
        Set[str]: set of artist names
    """
    cur = mydb.cursor()
    cur.execute(
        """
        SELECT DISTINCT a.name
        FROM Artist AS a
        WHERE EXISTS (
            SELECT 1 FROM Album AS al
            WHERE al.artist_id = a.artist_id
        )
        AND EXISTS (
            SELECT 1 FROM Song AS s
            WHERE s.artist_id = a.artist_id
              AND s.album_id IS NULL   -- single
        )
        """
    )
    return {name for (name,) in cur.fetchall()}


# ----------------------
# 8. 加载用户
# ----------------------

def load_users(mydb, users: List[str]) -> Set[str]:
    """
    Add users to the database.

    Args:
        mydb: database connection
        users: list of usernames

    Returns:
        Set[str]: set of all usernames that were not added (rejected) because
        they are duplicates of existing users or duplicates within the input list.
    """
    rejected: Set[str] = set()
    cur = mydb.cursor()

    seen_in_batch: Set[str] = set()

    for username in users:
        # 本次函数调用内部的重复，直接 reject
        if username in seen_in_batch:
            rejected.add(username)
            continue
        seen_in_batch.add(username)

        # 数据库里已有，也 reject
        cur.execute("SELECT user_id FROM User WHERE username = %s", (username,))
        if cur.fetchone():
            rejected.add(username)
            continue

        # 否则插入
        cur.execute("INSERT INTO User (username) VALUES (%s)", (username,))

    mydb.commit()
    return rejected


# ----------------------
# 9. 加载评分
# ----------------------

def load_song_ratings(
    mydb,
    song_ratings: List[Tuple[str, Tuple[str, str], int, str]],
) -> Set[Tuple[str, str, str]]:
    """
    Load ratings for songs, which are either singles or songs in albums.

    Args:
        mydb: database connection
        song_ratings: list of rating tuples of the form:
            (rater, (artist, song), rating, date)

    Returns:
        Set[Tuple[str,str,str]]: set of (username,artist,song) tuples that are rejected, for any of the following
        reasons:
        (a) username (rater) is not in the database, or
        (b) username is in database but (artist,song) combination is not in the database, or
        (c) username has already rated (artist,song) combination, or
        (d) everything else is legit, but rating is not in range 1..5
    """
    rejected: Set[Tuple[str, str, str]] = set()
    cur = mydb.cursor()

    for username, (artist_name, song_title), rating, rating_date in song_ratings:
        key = (username, artist_name, song_title)

        # (a) 用户不存在
        user_id = _get_user_id(mydb, username)
        if user_id is None:
            rejected.add(key)
            continue

        # (b) 歌曲不存在（通过 artist + title）
        artist_id = _get_artist_id(mydb, artist_name)
        if artist_id is None:
            rejected.add(key)
            continue

        song_id = _get_song_id_by_artist_and_title(mydb, artist_id, song_title)
        if song_id is None:
            rejected.add(key)
            continue

        # (d) rating 不在 [1,5]
        if not (1 <= rating <= 5):
            rejected.add(key)
            continue

        # (c) 这个用户已经评过这首歌
        cur.execute(
            "SELECT 1 FROM Rating WHERE user_id = %s AND song_id = %s",
            (user_id, song_id),
        )
        if cur.fetchone():
            rejected.add(key)
            continue

        # 合法评分，插入
        cur.execute(
            """
            INSERT INTO Rating (user_id, song_id, rating, rating_date)
            VALUES (%s, %s, %s, %s)
            """,
            (user_id, song_id, rating, rating_date),
        )

    mydb.commit()
    return rejected


# ----------------------
# 10. 被评分次数最多的歌曲
# ----------------------

def get_most_rated_songs(
    mydb,
    year_range: Tuple[int, int],
    n: int,
) -> List[Tuple[str, str, int]]:
    """
    Get the top n most rated songs in the given year range (both inclusive),
    ranked from most rated to least rated.
    "Most rated" refers to number of ratings, not actual rating scores.
    Ties are broken in alphabetical order of song title. If the number of rated songs is less
    than n, all rated songs are returned.
    """
    start_year, end_year = year_range
    cur = mydb.cursor()
    cur.execute(
        """
        SELECT s.title, a.name, COUNT(*) AS num_ratings
        FROM Rating AS r
        JOIN Song AS s ON r.song_id = s.song_id
        JOIN Artist AS a ON s.artist_id = a.artist_id
        WHERE YEAR(r.rating_date) BETWEEN %s AND %s
        GROUP BY r.song_id, s.title, a.name
        ORDER BY num_ratings DESC, s.title ASC
        LIMIT %s
        """,
        (start_year, end_year, n),
    )
    rows = cur.fetchall()
    return [(title, artist_name, num) for (title, artist_name, num) in rows]


# ----------------------
# 11. 最活跃的用户
# ----------------------

def get_most_engaged_users(
    mydb,
    year_range: Tuple[int, int],
    n: int,
) -> List[Tuple[str, int]]:
    """
    Get the top n most engaged users, in terms of number of songs they have rated.
    Break ties by alphabetical order of usernames.
    """
    start_year, end_year = year_range
    cur = mydb.cursor()
    cur.execute(
        """
        SELECT u.username, COUNT(*) AS num_rated
        FROM Rating AS r
        JOIN User AS u ON r.user_id = u.user_id
        WHERE YEAR(r.rating_date) BETWEEN %s AND %s
        GROUP BY r.user_id, u.username
        ORDER BY num_rated DESC, u.username ASC
        LIMIT %s
        """,
        (start_year, end_year, n),
    )
    rows = cur.fetchall()
    return [(username, num) for (username, num) in rows]


def main():
    # 可以在这里写你自己的测试代码
    pass


if __name__ == "__main__":
    main()