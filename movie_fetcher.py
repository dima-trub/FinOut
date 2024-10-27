import aiohttp
import aiosqlite
import asyncio
import logging
from typing import List, Dict, Optional, Tuple
from config import Config
from datetime import datetime

# logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AsyncMovieDatabase:
    def __init__(self):
        self.db_name = Config.db_name
        self.fetch_url = Config.fetch_url

    async def __aenter__(self):
        await self.init_db()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def init_db(self) -> None:

        async with aiosqlite.connect(self.db_name) as db:
            await db.execute('''
            CREATE TABLE IF NOT EXISTS movies (
                imdb_title_id VARCHAR(15) PRIMARY KEY,
                title VARCHAR(255),
                original_title VARCHAR(255),
                year INT,
                date_published DATE,
                genre VARCHAR(255),
                duration INT,
                country VARCHAR(100),
                language VARCHAR(100),
                director VARCHAR(255),
                writer VARCHAR(255),
                production_company VARCHAR(255),
                actors TEXT,
                description TEXT,
                avg_vote FLOAT,
                votes INT,
                budget VARCHAR(50),
                usa_gross_income VARCHAR(50),
                worldwide_gross_income VARCHAR(50),
                metascore FLOAT,
                reviews_from_users FLOAT,
                reviews_from_critics FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            await db.commit()

    async def fetch_movies(self) -> None:
        """Fetch movies from the given URL"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.fetch_url) as response:
                    if response.status != 200:
                        logger.error(f"Failed to fetch data. Status: {response.status}")
                        return

                    try:
                        data = await response.json()
                    except aiohttp.ContentTypeError as e:
                        logger.error(f"Failed to parse JSON. Error: {e}")
                        return

                    await self._insert_movies(data)

        except aiohttp.ClientError as e:
            logger.error(f"Error fetching data: {e}")

    async def _insert_movies(self, data) -> None:
        """Insert  data into the movies table."""
        try:
            movies = [self._get_movie_tuple(movie) for movie in (data if isinstance(data, list) else [data])]
            valid_movies = [movie for movie in movies if movie]

            if valid_movies:
                async with aiosqlite.connect(self.db_name) as db:
                    async with db.execute("BEGIN TRANSACTION;"):
                        await db.executemany('''
                        INSERT INTO movies (
                            imdb_title_id, title, original_title, year, date_published, genre, duration,
                            country, language, director, writer, production_company, actors, description,
                            avg_vote, votes, budget, usa_gross_income, worldwide_gross_income, metascore,
                            reviews_from_users, reviews_from_critics, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(imdb_title_id) DO UPDATE SET
                            title=excluded.title,
                            original_title=excluded.original_title,
                            year=excluded.year,
                            date_published=excluded.date_published,
                            genre=excluded.genre,
                            duration=excluded.duration,
                            country=excluded.country,
                            language=excluded.language,
                            director=excluded.director,
                            writer=excluded.writer,
                            production_company=excluded.production_company,
                            actors=excluded.actors,
                            description=excluded.description,
                            avg_vote=excluded.avg_vote,
                            votes=excluded.votes,
                            budget=excluded.budget,
                            usa_gross_income=excluded.usa_gross_income,
                            worldwide_gross_income=excluded.worldwide_gross_income,
                            metascore=excluded.metascore,
                            reviews_from_users=excluded.reviews_from_users,
                            reviews_from_critics=excluded.reviews_from_critics,
                            updated_at=CURRENT_TIMESTAMP
                        ''', valid_movies)
                    await db.commit()
                logger.info("Data inserted successfully.")
        except aiosqlite.Error as e:
            logger.error(f"Database insertion error: {e}")

    def _get_movie_tuple(self, movie: Dict) -> Optional[Tuple]:
        """Extract movie details as a tuple, if valid."""
        return (
            movie.get("imdb_title_id"),
            movie.get("title"),
            movie.get("original_title"),
            movie.get("year"),
            movie.get("date_published"),
            movie.get("genre"),
            movie.get("duration"),
            movie.get("country"),
            movie.get("language"),
            movie.get("director"),
            movie.get("writer"),
            movie.get("production_company"),
            movie.get("actors"),
            movie.get("description"),
            movie.get("avg_vote"),
            movie.get("votes"),
            movie.get("budget"),
            movie.get("usa_gross_income"),
            movie.get("worldwide_gross_income"),
            movie.get("metascore"),
            movie.get("reviews_from_users"),
            movie.get("reviews_from_critics"),
            datetime.now(),  # created_at
            datetime.now()   # updated_at
        )

    async def get_last_10_movie(self) -> List[Dict[str, Optional[str]]]:
        """Retrieve all movies from the database."""
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute("SELECT * FROM movies ORDER BY updated_at DESC LIMIT 10") as cursor:
                rows = await cursor.fetchall()
                return [
                    dict(zip([column[0] for column in cursor.description], row))
                    for row in rows
                ]

    async def get_last_movie(self,) -> Optional[Dict[str, Optional[str]]]:
        """Fetch a movie by its ID asynchronously."""
        async with aiosqlite.connect(self.db_name) as db:
            async with db.execute("SELECT * FROM movies ORDER BY updated_at DESC LIMIT 1") as cursor:
                row = await cursor.fetchone()
                if row:
                    return dict(zip([column[0] for column in cursor.description], row))
                return None

async def main() -> None:
    async with AsyncMovieDatabase() as movie_db:
        await movie_db.fetch_movies()

        # Example of retrieving all movies
        get_last_10_movie = await movie_db.get_last_10_movie()
        logger.info("Last 10 Movies: %s", get_last_10_movie)

        #Example of retrieving a specific movie by ID
        last_movie = await movie_db.get_last_movie()
        if last_movie:
            logger.info("Fetched Last Movie: %s", last_movie)
        else:
            logger.info("No movie found ")

if __name__ == "__main__":
    asyncio.run(main())





