import asyncio
import uvicorn
import logging
from data_collection.task_manager import TaskManager
import os
# Configura el directorio del archivo de log
log_file = os.path.join(os.path.dirname(__file__), 'app.log')

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


async def start_task_manager():
    while True:
        try:
            logger.info("Iniciando TaskManager")
            task_manager = TaskManager()
            await task_manager.start_data_collection()
        except Exception as e:
            logger.error("Error en TaskManager: %s", e)
            break


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


async def main():
    try:
        await asyncio.gather(start_task_manager(), start_uvicorn())
    except Exception as e:
        logger.critical("Error crítico: %s", e)
        logger.info("Cerrando el programa debido a un fallo crítico")

if __name__ == "__main__":
    asyncio.run(main())
