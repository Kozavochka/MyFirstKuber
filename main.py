import logging
import os
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from elastic_transport import ConnectionTimeout
from elasticsearch import ApiError, Elasticsearch
from fastapi import FastAPI, HTTPException
from redis import Redis
from redis.exceptions import RedisError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")

load_dotenv()

app = FastAPI()
redis = Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
elastic_url = os.getenv("ELASTIC_URL", "http://localhost:9200")
es = Elasticsearch(
    elastic_url,
    request_timeout=30,
    retry_on_timeout=True,
    max_retries=3,
)


@app.get("/health")
def health():
    logger.info("/health endpoint called")
    return {"status": "ok"}


@app.get("/print")
def print_log():
    logger.info("/print endpoint called")
    return {"status": "ok"}


@app.get("/point")
def point():
    logger.info("/point endpoint called")
    try:
        value = redis.incr("points")
    except RedisError as exc:
        logger.exception("redis error: %s", exc)
        return {"status": "error", "detail": "redis unavailable"}
    logger.info("points incremented: %s", value)
    return {"status": "ok", "points": value}


@app.post("/es/index/{index_name}")
def create_index(index_name: str, body: Optional[Dict[str, Any]] = None):
    logger.info("/es/index create called for index=%s", index_name)
    try:
        exists = es.indices.exists(index=index_name)
        if exists:
            logger.info("index already exists: %s", index_name)
            return {"status": "ok", "detail": "index already exists", "index": index_name}
        settings = (body or {}).get("settings")
        mappings = (body or {}).get("mappings")
        params: Dict[str, Any] = {}
        if settings:
            params["settings"] = settings
        if mappings:
            params["mappings"] = mappings
        es.indices.create(index=index_name, **params)
        logger.info("index created: %s", index_name)
        return {"status": "ok", "index": index_name}
    except ConnectionTimeout as exc:
        logger.exception("elasticsearch timeout: %s", exc)
        try:
            if es.indices.exists(index=index_name):
                return {
                    "status": "ok",
                    "index": index_name,
                    "detail": "index created but request timed out",
                }
        except ConnectionTimeout:
            pass
        raise HTTPException(status_code=504, detail="elasticsearch timeout")
    except ApiError as exc:
        logger.exception("elasticsearch error: %s", exc)
        raise HTTPException(status_code=502, detail="elasticsearch unavailable")


@app.get("/es/index")
def list_indices():
    logger.info("/es/index list called")
    try:
        indices = es.indices.get_alias(index="*")
        index_names = sorted(indices.keys())
        logger.info("indices fetched: count=%s", len(index_names))
        return {"status": "ok", "indices": index_names}
    except ConnectionTimeout as exc:
        logger.exception("elasticsearch timeout: %s", exc)
        raise HTTPException(status_code=504, detail="elasticsearch timeout")
    except ApiError as exc:
        logger.exception("elasticsearch error: %s", exc)
        raise HTTPException(status_code=502, detail="elasticsearch unavailable")


@app.post("/es/index/{index_name}/doc")
def create_document(index_name: str, document: dict):
    logger.info(
        "/es/index/{index_name}/doc called: index=%s keys=%s",
        index_name,
        sorted(document.keys()),
    )
    try:
        resp = es.index(index=index_name, document=document)
        logger.info(
            "document indexed: index=%s result=%s id=%s",
            index_name,
            resp.get("result"),
            resp.get("_id"),
        )
        return {"status": "ok", "result": resp.get("result"), "id": resp.get("_id")}
    except ConnectionTimeout as exc:
        logger.exception("elasticsearch timeout: %s", exc)
        raise HTTPException(status_code=504, detail="elasticsearch timeout")
    except ApiError as exc:
        logger.exception("elasticsearch error: %s", exc)
        raise HTTPException(status_code=502, detail="elasticsearch unavailable")


@app.get("/es/index/{index_name}/search")
def search_document(index_name: str, q: str):
    logger.info("/es/index/{index_name}/search called: index=%s query=%s", index_name, q)
    try:
        resp = es.search(index=index_name, query={"query_string": {"query": q}})
        hits = [hit.get("_source") for hit in resp.get("hits", {}).get("hits", [])]
        logger.info("search completed: index=%s hits=%s", index_name, len(hits))
        return {"status": "ok", "hits": hits}
    except ConnectionTimeout as exc:
        logger.exception("elasticsearch timeout: %s", exc)
        raise HTTPException(status_code=504, detail="elasticsearch timeout")
    except ApiError as exc:
        logger.exception("elasticsearch error: %s", exc)
        raise HTTPException(status_code=502, detail="elasticsearch unavailable")


@app.put("/es/index/{index_name}/settings")
def update_index_settings(index_name: str, settings: Dict[str, Any]):
    logger.info("/es/index/{index_name}/settings called: index=%s", index_name)
    try:
        es.indices.put_settings(index=index_name, settings=settings)
        logger.info("index settings updated: index=%s", index_name)
        return {"status": "ok", "index": index_name}
    except ConnectionTimeout as exc:
        logger.exception("elasticsearch timeout: %s", exc)
        raise HTTPException(status_code=504, detail="elasticsearch timeout")
    except ApiError as exc:
        logger.exception("elasticsearch error: %s", exc)
        raise HTTPException(status_code=502, detail="elasticsearch unavailable")


@app.put("/es/index/{index_name}/mapping")
def update_index_mapping(index_name: str, mappings: Dict[str, Any]):
    logger.info("/es/index/{index_name}/mapping called: index=%s", index_name)
    if "properties" not in mappings:
        logger.warning(
            "mapping update rejected for index=%s: no properties field",
            index_name,
        )
        raise HTTPException(
            status_code=400, detail="mappings must include 'properties' field"
        )
    try:
        es.indices.put_mapping(index=index_name, properties=mappings["properties"])
        logger.info("index mapping updated: index=%s", index_name)
        return {"status": "ok", "index": index_name}
    except ConnectionTimeout as exc:
        logger.exception("elasticsearch timeout: %s", exc)
        raise HTTPException(status_code=504, detail="elasticsearch timeout")
    except ApiError as exc:
        logger.exception("elasticsearch error: %s", exc)
        raise HTTPException(status_code=502, detail="elasticsearch unavailable")
