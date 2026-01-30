import logging

from fastapi import FastAPI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/print")
def print_log():
    logger.info("/print endpoint called")
    return {"status": "ok"}
