from contextlib import asynccontextmanager
from datetime import datetime
from typing import Annotated

from fastapi import FastAPI, Query

from performance_monitor.common_repo import (
    get_default_start_time_based_on_generation,
    get_monitoring_data,
    ensure_system_requirements,
)
from performance_monitor.db import init_db
from performance_monitor.ethernet_and_fiber_channel.model import Ethernet, FiberChannel
from performance_monitor.pool_and_lun.model import LUNData, PoolData


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Check if required system commands are available before starting the application
    ensure_system_requirements()
    init_db()
    yield


app = FastAPI(root_path="/performance", lifespan=lifespan)


@app.get("/fibre-channel")
async def get_fc_performance(
    *,
    fiber_channel_name: Annotated[str, Query()] = "",
    start: datetime | None = None,
    end: Annotated[datetime | None, Query(default_factory=lambda: datetime.now())],
    generation: Annotated[int, Query(ge=0, le=5)] = 0,
):
    if not start:
        start = get_default_start_time_based_on_generation(generation)
    results = await get_monitoring_data(
        fiber_channel_name,
        FiberChannel,
        generation,
        start,
        end,
    )

    return [results]


@app.get("/lun")
async def get_lun_performance(
    *,
    lun_name: Annotated[str, Query()] = "",
    start: datetime | None = None,
    end: Annotated[datetime | None, Query(default_factory=lambda: datetime.now())],
    generation: Annotated[int, Query(ge=0, le=5)] = 0,
):
    if not start:
        start = get_default_start_time_based_on_generation(generation)
    results = await get_monitoring_data(
        lun_name,
        LUNData,
        generation,
        start,
        end,
    )

    return [results]


@app.get("/pool")
async def get_pool_performance(
    *,
    pool_name: Annotated[str, Query()] = "",
    start: datetime | None = None,
    end: Annotated[datetime | None, Query(default_factory=lambda: datetime.now())],
    generation: Annotated[int, Query(ge=0, le=5)] = 0,
):
    if not start:
        start = get_default_start_time_based_on_generation(generation)
    results = await get_monitoring_data(
        pool_name,
        PoolData,
        generation,
        start,
        end,
    )

    return [results]


@app.get("/ethernet")
async def get_ethernet_performance(
    *,
    network_name: Annotated[str, Query(pattern="^(Primary|Secondary|enp7s0f\\d)$")] = "",
    start: datetime | None = None,
    end: Annotated[datetime | None, Query(default_factory=lambda: datetime.now())],
    generation: Annotated[int, Query(ge=0, le=5)] = 0,
):
    if not start:
        start = get_default_start_time_based_on_generation(generation)
    network_name = "eno1" if network_name == "Primary" else "eno2" if network_name == "Secondary" else network_name

    results = await get_monitoring_data(
        network_name,
        Ethernet,
        generation,
        start,
        end,
    )

    return [results]
