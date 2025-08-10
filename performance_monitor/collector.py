import asyncio
from datetime import datetime
from time import time

from performance_monitor.config import REAL_TIME_INTERVAL as DEFAULT_WAITING_TIME
from performance_monitor.db import init_db
from performance_monitor.common_repo import ensure_system_requirements
from performance_monitor.pool_and_lun.pool_and_lun import pool_and_lun_job
from performance_monitor.ethernet_and_fiber_channel.ethernet_and_fiber_channel import fiber_channel_and_ethernet_job
from performance_monitor.cleaner_job import clean_old_data


def discontinuous_collector_task(interval: int = DEFAULT_WAITING_TIME):
    def decorator(func):
        async def wrapper():
            while True:
                print(f"{func.__name__} started to collect data at {datetime.now()}")
                now = time()
                await asyncio.gather(
                    func(),
                    asyncio.sleep(interval),
                )
                print(f"{func.__name__} finished in {time() - now} seconds at {datetime.now()}")

        return wrapper

    return decorator


async def cleaner_job():
    while True:
        await asyncio.gather(
            clean_old_data(),
            asyncio.sleep(60 * 60),
        )


@discontinuous_collector_task()
async def collector_pool_and_lun():
    await pool_and_lun_job()


async def collector_fiber_channel_and_ethernet():
    await fiber_channel_and_ethernet_job()


async def main():
    # Check if required system commands are available before starting the collector
    ensure_system_requirements()
    init_db()
    await asyncio.gather(
        collector_pool_and_lun(),
        collector_fiber_channel_and_ethernet(),
        cleaner_job(),
    )


if __name__ == "__main__":
    asyncio.run(main())
