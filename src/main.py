import asyncio
import logging
from logging.handlers import RotatingFileHandler
from data_collection.task_manager import TaskManager
import os

os.makedirs("logs", exist_ok=True)
log_file = os.path.join("logs", "app.log")
r_handler = RotatingFileHandler(
    log_file, maxBytes=5000000, backupCount=1)
r_handler.setLevel(logging.INFO)
r_handler.setFormatter(logging.Formatter(
    "%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
logging.basicConfig(level=logging.INFO, handlers=[r_handler])
logger = logging.getLogger(__name__)


async def main():
    while True:
        try:
            logger.info("RUN - MAIN")
            await TaskManager().start_data_collection()
        except Exception as e:
            logger.error("Error en main: %s", e)
            break

if __name__ == "__main__":
    asyncio.run(main())


# def start_uvicorn():
#     try:
#         config = uvicorn.Config(
#             "api.app:app", host="0.0.0.0", port=8000, loop="asyncio")
#         server = uvicorn.Server(config)
#         logger.info("Iniciando Uvicorn")
#         server.serve()
#     except Exception as e:
#         logger.error("Error en Uvicorn: %s", e)
#         raise
