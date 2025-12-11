"""
简单的本地测试脚本，用于测试 music_db.py 中所有函数是否可以正常工作。

使用方式：
    1. 确保已经在 MySQL 中创建了数据库 music_db，并导入了 music_db.sql。
    2. 修改 get_connection() 中的数据库连接参数。
    3. 在命令行运行：  python test_music_db.py
"""

from typing import List, Tuple, Set
import mysql.connector

from music_db import (
    clear_database,
    load_single_songs,
    load_albums,
    load_users,
    load_song_ratings,
    get_most_prolific_individual_artists,
    get_artists_last_single_in_year,
    get_top_song_genres,
    get_album_and_single_artists,
    get_most_rated_songs,
    get_most_engaged_users,
)


def get_connection():
    """
    根据你自己的环境修改这些连接参数。
    database 名字一定要是你已经执行过 music_db.sql 的那个库。
    """
    return mysql.connector.connect(
        host="localhost",
        user="root",          # 修改为你的 MySQL 用户名
        password="20050305", # 修改为你的 MySQL 密码
        database="music_db",  # 建议用和 autograder 一样的名字 music_db
        autocommit=False,
    )


def print_section(title: str):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def run_tests():
    db = get_connection()
    try:
        # 1. 清空数据库
        print_section("1. clear_database 测试")
        clear_database(db)
        print("数据库已清空。")

        # 2. 载入用户
        print_section("2. load_users 测试")
        users = ["user1", "user2", "user3"]
        rejected_users = load_users(db, users)
        print("load_users rejected:", rejected_users)
        assert rejected_users == set(), "不应该有被拒绝的用户"

        # 3. 载入单曲（single_songs）
        #    每条记录: (title, (genres...), artist_name, release_date_str)
        print_section("3. load_single_songs 测试")
        single_songs = [
            # Alice 有两首 2020 年的单曲
            ("Sky", ("Pop",), "Alice", "2020-01-01"),
            ("Rock Me", ("Rock", "Pop"), "Alice", "2020-06-15"),
            # Bob 有一首 2021 年单曲
            ("Jazz Night", ("Jazz",), "Bob", "2021-02-20"),
            # Carl 有一首 2019 年单曲
            ("Old Hit", ("Rock",), "Carl", "2019-08-30"),
        ]
        rejected_singles = load_single_songs(db, single_songs)
        print("load_single_songs rejected:", rejected_singles)
        assert rejected_singles == set(), "不应该有被拒绝的单曲（测试数据都合法）"

        # 4. 载入专辑（albums）
        #    每条记录: (album_title, album_genre, artist_name, release_date_str, [song_titles])
        print_section("4. load_albums 测试")
        albums = [
            # Alice 有一张 Pop 专辑
            ("Alice Album", "Pop", "Alice", "2019-12-01", ["AlbumSong1", "AlbumSong2"]),
            # Bob 有一张 Jazz 专辑
            ("Bob Debut", "Jazz", "Bob", "2020-10-10", ["Smooth", "Late Night"]),
        ]
        rejected_albums = load_albums(db, albums)
        print("load_albums rejected:", rejected_albums)
        assert rejected_albums == set(), "不应该有被拒绝的专辑（测试数据都合法）"

        # 5. 载入评分（song_ratings）
        #    每条记录: (username, (artist_name, song_title), rating, rating_date_str)
        print_section("5. load_song_ratings 测试")
        song_ratings = [
            ("user1", ("Alice", "Sky"), 5, "2020-01-10"),
            ("user2", ("Alice", "Sky"), 3, "2020-02-10"),
            ("user1", ("Alice", "Rock Me"), 4, "2020-06-20"),
            ("user2", ("Bob", "Jazz Night"), 5, "2021-03-01"),
            ("user3", ("Bob", "Jazz Night"), 4, "2021-03-02"),
            ("user3", ("Carl", "Old Hit"), 2, "2019-09-01"),
        ]
        rejected_ratings = load_song_ratings(db, song_ratings)
        print("load_song_ratings rejected:", rejected_ratings)
        assert rejected_ratings == set(), "不应该有被拒绝的评分（测试数据都合法）"

        # 到这里：数据库里已经有：
        # - 3 个用户
        # - 3 个 artist：Alice / Bob / Carl（和专辑中的 artist 一起至少 3 个）
        # - 4 首单曲 + 4 首专辑歌曲（2+2）
        # - 3 个 genre：Pop / Rock / Jazz
        # - 6 条评分

        # 6. 测试 get_most_prolific_individual_artists
        print_section("6. get_most_prolific_individual_artists 测试")
        # 统计 2019-2021 年期间发行单曲数量最多的前 3 名
        prolific = get_most_prolific_individual_artists(db, 3, (2019, 2021))
        print("结果:", prolific)
        # 在我们的数据里，单曲情况：
        # Alice: Sky(2020), Rock Me(2020) -> 2
        # Bob: Jazz Night(2021)          -> 1
        # Carl: Old Hit(2019)            -> 1
        assert len(prolific) >= 1, "结果不应为空"
        assert prolific[0][0] == "Alice" and prolific[0][1] == 2, "Alice 应该是单曲数量最多的 artist"

        # 7. 测试 get_artists_last_single_in_year
        print_section("7. get_artists_last_single_in_year 测试")
        last_single_2020 = get_artists_last_single_in_year(db, 2020)
        print("2020 年最后一首单曲的 artist 集合:", last_single_2020)
        # 对 Alice 来说，其单曲中最大日期为 2020-06-15，年份为 2020；
        # Bob 的最后单曲年份是 2021；
        # Carl 的最后单曲年份是 2019。
        assert "Alice" in last_single_2020, "Alice 应该出现在 2020 年的 last single artist 集合中"
        assert "Bob" not in last_single_2020, "Bob 的 last single 不在 2020 年"
        assert "Carl" not in last_single_2020, "Carl 的 last single 不在 2020 年"

        # 8. 测试 get_top_song_genres
        print_section("8. get_top_song_genres 测试")
        top_genres = get_top_song_genres(db, 10)
        print("top genres:", top_genres)
        # 计算一下每个 genre 下的歌曲数量（按当前测试数据）：
        # Pop: Sky, Rock Me, AlbumSong1, AlbumSong2 => 4
        # Rock: Rock Me, Old Hit                   => 2
        # Jazz: Jazz Night, Smooth, Late Night     => 3
        # 因此第一名应该是 Pop，数量 4
        assert len(top_genres) >= 1, "结果不应为空"
        assert top_genres[0][0] == "Pop" and top_genres[0][1] == 4, "Pop 应该是歌最多的 genre"

        # 9. 测试 get_album_and_single_artists
        print_section("9. get_album_and_single_artists 测试")
        album_and_single_artists = get_album_and_single_artists(db)
        print("同时有专辑和单曲的 artists:", album_and_single_artists)
        # 在我们的数据中：
        # - Alice 有专辑 (Alice Album) 也有单曲 (Sky, Rock Me)
        # - Bob   有专辑 (Bob Debut)   也有单曲 (Jazz Night)
        assert "Alice" in album_and_single_artists, "Alice 应该同时有专辑和单曲"
        assert "Bob" in album_and_single_artists, "Bob 应该同时有专辑和单曲"

        # 10. 测试 get_most_rated_songs
        print_section("10. get_most_rated_songs 测试")
        most_rated_songs = get_most_rated_songs(db, (2019, 2021), 10)
        print("most rated songs (2019-2021):", most_rated_songs)
        # 评分统计（只看 2019-2021 期间；所有评分都在这个区间内）：
        # Alice - Sky        : 2 次
        # Alice - Rock Me    : 1 次
        # Bob   - Jazz Night : 2 次
        # Carl  - Old Hit    : 1 次
        # 因此评分数最多的是 Sky 和 Jazz Night，两首歌各 2 次。
        titles_with_2 = {t for (t, a, c) in most_rated_songs if c == 2}
        assert {"Sky", "Jazz Night"}.issubset(titles_with_2), "Sky 和 Jazz Night 都应该有 2 条评分"

        # 11. 测试 get_most_engaged_users
        print_section("11. get_most_engaged_users 测试")
        most_engaged_users = get_most_engaged_users(db, (2019, 2021), 10)
        print("most engaged users (2019-2021):", most_engaged_users)
        # 评分次数：
        # user1: 2 次 (Sky, Rock Me)
        # user2: 2 次 (Sky, Jazz Night)
        # user3: 2 次 (Jazz Night, Old Hit)
        # 三个用户的评分次数都相同，都是 2。
        assert len(most_engaged_users) == 3, "应该有 3 个用户有评分"
        assert all(cnt == 2 for (_, cnt) in most_engaged_users), "每个用户的评分次数都应为 2"

        print_section("所有基本测试通过 ✔")
        print("如果你看到了这些输出，说明各个函数在这组测试数据上运行正常。")

    finally:
        db.close()


if __name__ == "__main__":
    run_tests()