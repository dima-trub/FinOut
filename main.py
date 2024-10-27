from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from databases import Database
import logging
import uvicorn
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.executors.pool import ThreadPoolExecutor
from movie_fetcher import AsyncMovieDatabase
import traceback
from config import Config


logging.basicConfig(level=logging.INFO)


database = Database(Config.DATABASE_URL)


class SchedulerManager:
    def __init__(self):
        self.scheduler = AsyncIOScheduler(executors={'default': ThreadPoolExecutor(1)})

    def start_scheduler(self, job_function, interval_seconds: int) -> None:
        try:
            self.scheduler.add_job(job_function, "interval", seconds=interval_seconds)
            self.scheduler.start()
            logging.info("Scheduler started successfully.")
        except Exception as e:
            logging.error(f"Failed to start scheduler: {e}")

    def stop_scheduler(self) -> None:
        try:
            self.scheduler.shutdown()
            logging.info("Scheduler stopped successfully.")
        except Exception as e:
            logging.error(f"Failed to stop scheduler: {e}")


class AppManager:
    def __init__(self, db: Database):
        self.db = db
        self.scheduler_manager = SchedulerManager()
        self.async_movie_db = AsyncMovieDatabase()

    async def fetch_and_store_movies(self):
        try:
            await self.async_movie_db.fetch_movies()
            logging.info("Movies fetched and stored successfully.")
        except Exception as e:
            logging.error(f"Failed to fetch and store movies: {e}\n{traceback.format_exc()}")

    def start_scheduler(self) -> None:
        self.scheduler_manager.start_scheduler(self.fetch_and_store_movies, Config.SCHEDULER_INTERVAL)

    def stop_scheduler(self) -> None:
        self.scheduler_manager.stop_scheduler()



app_manager = AppManager(database)



@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        logging.info("Starting application...")
        await database.connect()
        await app_manager.async_movie_db.init_db()
        app_manager.start_scheduler()
        logging.info("Application started successfully.")
        yield
    except Exception as e:
        logging.error(f"Error during startup: {e}")
        raise
    finally:
        try:
            await database.disconnect()
            app_manager.stop_scheduler()
            logging.info("Application shutdown completed.")
        except Exception as e:
            logging.error(f"Error during shutdown: {e}")



app = FastAPI(lifespan=lifespan)


@app.get("/last_10_movies")
async def get_last_10_movies() -> JSONResponse:
    try:
        movies = await app_manager.async_movie_db.get_last_10_movie()
        movie_list = [
            {
                "imdb_title_id": movie["imdb_title_id"],
                "title": movie['title'],
                "original_title": movie['original_title'],
                "year": movie['year'],
                "date_published": movie['date_published'],
                "genre": movie['genre'],
                "duration": movie['duration'],
                "country": movie['country'],
                "language": movie['language'],
                "director": movie["director"],
                "writer": movie['writer'],
                "production_company": movie['production_company'],
                "actors": movie['actors'],
                "description": movie['description'],
                "avg_vote": movie['avg_vote'],
                "votes": movie['votes'],
                "budget": movie["budget"],
                "usa_gross_income": movie['usa_gross_income'],
                "worldwide_gross_income": movie['worldwide_gross_income'],
                "metascore": movie['metascore'],
                "reviews_from_users": movie['reviews_from_users'],
                "reviews_from_critics": movie['reviews_from_critics']
            } for movie in movies
        ]

        if not movie_list:
            logging.info("No movies available in the database.")
            return JSONResponse(content=[], status_code=204)

        return JSONResponse(content=movie_list)
    except Exception as e:
        logging.error(f"Error fetching movies: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal Server Error")


@app.get("/last_movie")
async def get_last_movie() -> JSONResponse:
    try:
        movie = await app_manager.async_movie_db.get_last_movie()

        if movie:
            return JSONResponse(content={
                "imdb_title_id": movie.get("imdb_title_id"),
                "title": movie.get("title"),
                "original_title": movie.get("original_title"),
                "year": movie.get("year"),
                "date_published": movie.get("date_published"),
                "genre": movie.get("genre"),
                "duration": movie.get("duration"),
                "country": movie.get("country"),
                "language": movie.get("language"),
                "director": movie.get("director"),
                "writer": movie.get("writer"),
                "production_company": movie.get("production_company"),
                "actors": movie.get("actors"),
                "description": movie.get("description"),
                "avg_vote": movie.get("avg_vote"),
                "votes": movie.get("votes"),
                "budget": movie.get("budget"),
                "usa_gross_income": movie.get("usa_gross_income"),
                "worldwide_gross_income": movie.get("worldwide_gross_income"),
                "metascore": movie.get("metascore"),
                "reviews_from_users": movie.get("reviews_from_users"),
                "reviews_from_critics": movie.get("reviews_from_critics")
            })
        else:
            raise HTTPException(status_code=404, detail="Movie not found")
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error fetching movie: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail="Internal Server Error")



if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)









