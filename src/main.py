import asyncio
import uvicorn
from data_collection.task_manager import TaskManager
# "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d","3d","1w",


async def start_task_manager():
    while True:
        try:
            task_manager = TaskManager()
            await task_manager.start_data_collection()
        except:
            print("REINICIANDO RECOLECION DE DATOS")
            await asyncio.sleep(120)
            task_manager = TaskManager()
            await task_manager.start_data_collection()


async def start_uvicorn():
    config = uvicorn.Config("api.app:app", host="0.0.0.0",
                            port=8000, loop="asyncio")
    server = uvicorn.Server(config)
    await server.serve()


async def main():
    await asyncio.gather(start_task_manager(), start_uvicorn())

if __name__ == "__main__":
    asyncio.run(main())
