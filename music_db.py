from typing import Tuple, List, Set


# ----------------------
# Helper functions
# ----------------------

def _get_or_create_artist(mydb, artist_name: str) -> int:
    """
    Return artist_id; if the artist does not exist, create it.

    Assumes the Artist table has the following schema:
        Artist(artist_id PK AUTO_INCREMENT, name UNIQUE NOT NULL)
    (There is no is_group column.)
    """
    cur = mydb.cursor()
    cur.execute("SELECT artist_id FROM Artist WHERE name = %s", (artist_name,))
    row = cur.fetchone()
    if row:
        return row[0]

    # Insert new artist
    cur.execute("INSERT INTO Artist (name) VALUES (%s)", (artist_name,))
    mydb.commit()
    return cur.lastrowid


def _get_artist_id(mydb, artist_name: str):
    """Return artist_id; if not found, return None."""
    cur = mydb.cursor()
    cur.execute("SELECT artist_id FROM Artist WHERE name = %s", (artist_name,))
    row = cur.fetchone()
    return row[0] if row else None


def _get_or_create_genre(mydb, genre_name: str) -> int:
    """Return genre_id; create the genre if it does not exist."""
    cur = mydb.cursor()
    cur.execute("SELECT genre_id FROM Genre WHERE name = %s", (genre_name,))
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("INSERT INTO Genre (name) VALUES (%s)", (genre_name,))
    mydb.commit()
    return cur.lastrowid


def _get_song_id_by_artist_and_title(mydb, artist_id: int, title: str):
    """Look up song_id by (artist_id, title); return None if not found."""
    cur = mydb.cursor()
    cur.execute(
        "SELECT song_id FROM Song WHERE artist_id = %s AND title = %s",
        (artist_id, title),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _get_user_id(mydb, username: str):
    """Return user_id; return None if not found."""
    cur = mydb.cursor()
    cur.execute("SELECT user_id FROM User WHERE username = %s", (username,))
    row = cur.fetchone()
    return row[0] if row else None


# ----------------------
# 1. Clear all tables in the database
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

    # Delete child tables that have foreign keys before deleting parent tables
    tables = ["Rating", "SongGenre", "Song", "Album", "User", "Genre", "Artist"]
    for t in tables:
        cur.execute(f"DELETE FROM {t}")
    mydb.commit()


# ----------------------
# 2. Load single songs
# ----------------------

def load_single_songs(
    mydb, single_songs: List[Tuple[str, Tuple[str, ...], str, str]]
) -> Set[Tuple[str, str]]:
    """
    Add single songs to the database.

    Single song: a Song with album_id = NULL. A song can have multiple genres,
    implemented via the SongGenre table.

    If a single song has no genre(s) in the input, it is rejected and not added.

    Args:
        mydb: database connection
        single_songs: list of 4-tuples:
            (song_title, genres, artist_name, release_date_string)

    Returns:
        Set[Tuple[str,str]]: (song_title, artist_name) pairs that were rejected,
        either because:
          - this artist already has a song with this title (single or in album), or
          - the input for this single had no genres
    """
    rejected: Set[Tuple[str, str]] = set()
    cur = mydb.cursor()

    for title, genre_names, artist_name, release_date in single_songs:
        # Single song: must have at least one genre; otherwise reject
        if not genre_names:
            rejected.add((title, artist_name))
            continue

        # Create or fetch the artist
        artist_id = _get_or_create_artist(mydb, artist_name)

        # Check whether this (artist, title) song already exists
        cur.execute(
            "SELECT song_id FROM Song WHERE artist_id = %s AND title = %s",
            (artist_id, title),
        )
        row = cur.fetchone()
        if row:
            rejected.add((title, artist_name))
            continue

        # Insert the single (album_id is NULL; under strategy B Song has no genre_id column)
        cur.execute(
            """
            INSERT INTO Song (title, artist_id, album_id, release_date)
            VALUES (%s, %s, NULL, %s)
            """,
            (title, artist_id, release_date),
        )
        song_id = cur.lastrowid

        # Insert all genres for this single into SongGenre (a song may have multiple genres)
        # Use a set to deduplicate genres to avoid violating the (song_id, genre_id) primary key
        unique_genres = set(genre_names)
        for gname in unique_genres:
            genre_id = _get_or_create_genre(mydb, gname)
            cur.execute(
                "INSERT INTO SongGenre (song_id, genre_id) VALUES (%s, %s)",
                (song_id, genre_id),
            )

    mydb.commit()
    return rejected


# ----------------------
# 3. Most prolific individual artists (by number of singles)
# ----------------------

def get_most_prolific_individual_artists(
    mydb,
    n: int,
    year_range: Tuple[int, int],
) -> List[Tuple[str, int]]:
    """
    Get the top n most prolific individual artists by number of singles released in a year range.
    Break ties by alphabetical order of artist name.

    Since the Artist table no longer has an is_group column, we treat all artists as individuals here.

    Args:
        mydb: database connection
        n: how many to get
        year_range: tuple, e.g. (2015,2020)

    Returns:
        List[Tuple[str,int]]: list of (artist name, number of songs) tuples.
    """
    start_year, end_year = year_range
    cur = mydb.cursor()

    # Count only singles: album_id IS NULL
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
# 4. Artists whose last single is in the given year
# ----------------------

def get_artists_last_single_in_year(mydb, year: int) -> Set[str]:
    """
    Get all artists who released their last single in the given year.

    Args:
        mydb: database connection
        year: the year of interest

    Returns:
        Set[str]: artist names.
    """
    cur = mydb.cursor()

    # For each artist, compute the max release_date over all singles and then filter by year
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
    return {name for (name, _) in cur.fetchall()}


# ----------------------
# 5. Load albums
# ----------------------

def load_albums(
    mydb,
    albums: List[Tuple[str, str, str, str, List[str]]],
) -> Set[Tuple[str, str]]:
    """
    Add album information to the database.

    Args:
        mydb: database connection
        albums: list of tuples:
            (album_title, album_genre, artist_name, release_date_string, song_titles)

    Returns:
        Set[Tuple[str,str]]: set of (album_title, artist_name) that could not be added
        because there is already an album with the same title for that artist.
    """
    rejected: Set[Tuple[str, str]] = set()
    cur = mydb.cursor()

    for album_title, album_genre, artist_name, release_date, song_titles in albums:
        # Create or fetch the artist
        artist_id = _get_or_create_artist(mydb, artist_name)
        # Album genre (Album still has a genre_id column)
        genre_id = _get_or_create_genre(mydb, album_genre)

        # Check whether this artist already has an album with the same title
        cur.execute(
            "SELECT album_id FROM Album WHERE artist_id = %s AND title = %s",
            (artist_id, album_title),
        )
        if cur.fetchone():
            rejected.add((album_title, artist_name))
            continue

        # Insert the album
        cur.execute(
            """
            INSERT INTO Album (title, artist_id, release_date, genre_id)
            VALUES (%s, %s, %s, %s)
            """,
            (album_title, artist_id, release_date, genre_id),
        )
        album_id = cur.lastrowid

        # All songs in the album must have the same genre as the album
        for song_title in song_titles:
            # Avoid conflicts: if this (artist, title) song already exists, skip this track (do not reject the whole album)
            cur.execute(
                """
                SELECT song_id FROM Song
                WHERE artist_id = %s AND title = %s
                """,
                (artist_id, song_title),
            )
            row = cur.fetchone()
            if row:
                # Song already exists, skip
                continue

            # Insert the Song row (with no genre_id column)
            cur.execute(
                """
                INSERT INTO Song (title, artist_id, album_id, release_date)
                VALUES (%s, %s, %s, %s)
                """,
                (song_title, artist_id, album_id, release_date),
            )
            song_id = cur.lastrowid

            # Insert one row into SongGenre with the album genre
            cur.execute(
                "INSERT INTO SongGenre (song_id, genre_id) VALUES (%s, %s)",
                (song_id, genre_id),
            )

    mydb.commit()
    return rejected


# ----------------------
# 6. Genres with the largest number of songs
# ----------------------

def get_top_song_genres(mydb, n: int) -> List[Tuple[str, int]]:
    """
    Get the top n genres by number of songs.
    A song with multiple genres contributes once to each genre.

    Under strategy B, song-genre relationships are stored in SongGenre, so we must join through SongGenre to reach Genre.

    Args:
        mydb: database connection
        n: number of genres

    Returns:
        List[Tuple[str,int]]: (genre, number_of_songs), sorted by number_of_songs desc, and by genre name ascending to break ties.
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
# 7. Artists who have both albums and singles
# ----------------------

def get_album_and_single_artists(mydb) -> Set[str]:
    """
    Get artists who have released albums as well as singles.

    Args:
        mydb: database connection

    Returns:
        Set[str]: artist names.
    """
    cur = mydb.cursor()
    cur.execute(
        """
        SELECT DISTINCT a.name
        FROM Artist AS a
        WHERE EXISTS (
            SELECT 1
            FROM Album AS al
            WHERE al.artist_id = a.artist_id
        )
        AND EXISTS (
            SELECT 1
            FROM Song AS s
            WHERE s.artist_id = a.artist_id
              AND s.album_id IS NULL   -- single
        )
        """
    )
    return {name for (name,) in cur.fetchall()}


# ----------------------
# 8. Load users
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
        # Reject duplicates within this function call (within the input list)
        if username in seen_in_batch:
            rejected.add(username)
            continue
        seen_in_batch.add(username)

        # If the username already exists in the database, also reject
        cur.execute("SELECT user_id FROM User WHERE username = %s", (username,))
        if cur.fetchone():
            rejected.add(username)
            continue

        # Otherwise insert the new user
        cur.execute("INSERT INTO User (username) VALUES (%s)", (username,))

    mydb.commit()
    return rejected


# ----------------------
# 9. Load song ratings
# ----------------------

def load_song_ratings(
    mydb,
    song_ratings: List[Tuple[str, Tuple[str, str], int, str]],
) -> Set[Tuple[str, str, str]]:
    """
    Add song ratings to the database.

    A rating is applied to a song (single or album track).
    It must satisfy:
        - user exists
        - song exists
        - user has not already rated this song
        - rating in 1..5

    Args:
        mydb: database connection
        song_ratings: list of tuples:
            (username, (artist_name, song_title), rating, rating_date_string)

    Returns:
        Set of (username, artist_name, song_title) that were rejected, because:
        (a) username not in database, or
        (b) username is in database but (artist,song) combination is not in the database, or
        (c) username has already rated (artist,song) combination, or
        (d) everything else is legit, but rating is not in range 1..5
    """
    rejected: Set[Tuple[str, str, str]] = set()
    cur = mydb.cursor()

    for username, (artist_name, song_title), rating, rating_date in song_ratings:
        key = (username, artist_name, song_title)

        # (a) User does not exist
        user_id = _get_user_id(mydb, username)
        if user_id is None:
            rejected.add(key)
            continue

        # (b) Song does not exist (by artist + title)
        artist_id = _get_artist_id(mydb, artist_name)
        if artist_id is None:
            rejected.add(key)
            continue

        song_id = _get_song_id_by_artist_and_title(mydb, artist_id, song_title)
        if song_id is None:
            rejected.add(key)
            continue

        # (d) rating is not in [1,5]
        if not (1 <= rating <= 5):
            rejected.add(key)
            continue

        # (c) This user has already rated this song
        cur.execute(
            "SELECT 1 FROM Rating WHERE user_id = %s AND song_id = %s",
            (user_id, song_id),
        )
        if cur.fetchone():
            rejected.add(key)
            continue

        # Rating is valid; insert it
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
# 10. Songs with the largest number of ratings
# ----------------------

def get_most_rated_songs(
    mydb,
    year_range: Tuple[int, int],
    n: int,
) -> List[Tuple[str, str, int]]:
    """
    Get the n songs which received the largest number of ratings during a given period.
    Break ties by alphabetical order by song title.

    A rating is counted in the year of rating_date (not the song's release date).

    Args:
        mydb: database connection
        year_range: (start_year, end_year)
        n: how many results to return

    Returns:
        A list of (song_title, artist_name, number_of_ratings).
    """
    start_year, end_year = year_range
    cur = mydb.cursor()

    cur.execute(
        """
        SELECT s.title, a.name, COUNT(*) AS num_ratings
        FROM Rating AS r
        JOIN Song   AS s ON r.song_id = s.song_id
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
# 11. Most engaged users
# ----------------------

def get_most_engaged_users(
    mydb,
    year_range: Tuple[int, int],
    n: int,
) -> List[Tuple[str, int]]:
    """
    Get the n users who gave ratings to the largest number of distinct songs in a given period.
    Ties are broken by username (alphabetically).

    Args:
        mydb: database connection
        year_range: (start_year, end_year)
        n: how many results to return

    Returns:
        A list of (username, number_of_ratings).
    """
    start_year, end_year = year_range
    cur = mydb.cursor()

    cur.execute(
        """
        SELECT u.username, COUNT(*) AS num_rated
        FROM Rating AS r
        JOIN User   AS u ON r.user_id = u.user_id
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
    # You can write your own ad-hoc tests here
    pass


if __name__ == "__main__":
    main()