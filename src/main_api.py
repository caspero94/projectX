import os
import logging
import uvicorn
import asyncio
from logging.handlers import RotatingFileHandler

os.makedirs("logs", exist_ok=True)
log_file = os.path.join("logs", "api.log")
r_handler = RotatingFileHandler(
    log_file, maxBytes=5000000, backupCount=1)
r_handler.setLevel(logging.DEBUG)
r_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
logging.basicConfig(level=logging.DEBUG, handlers=[r_handler])
logger = logging.getLogger(__name__)


async def start_uvicorn():
    try:
        config = uvicorn.Config(
            "api.app:app", host="0.0.0.0", port=8000, loop="asyncio")
        server = uvicorn.Server(config)
        logger.info("Iniciando Uvicorn")
        await server.serve()
    except Exception as e:
        logger.error("Error en Uvicorn: %s", e)
        raise


if __name__ == "__main__":
    asyncio.run(start_uvicorn())
