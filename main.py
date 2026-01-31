import logging
import os

from fastapi import FastAPI
from redis import Redis
from redis.exceptions import RedisError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

app = FastAPI()
redis = Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/print")
def print_log():
    logger.info("/print endpoint called")
    return {"status": "ok"}


@app.get("/point")
def point():
    try:
        value = redis.incr("points")
    except RedisError as exc:
        logger.exception("redis error: %s", exc)
        return {"status": "error", "detail": "redis unavailable"}
    return {"status": "ok", "points": value}
