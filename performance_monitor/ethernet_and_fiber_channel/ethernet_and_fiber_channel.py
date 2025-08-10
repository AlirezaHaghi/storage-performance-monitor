import asyncio
from datetime import datetime
from pathlib import Path
from time import time
from typing import NoReturn

import aiofiles
import psutil
from psutil._common import snetio

from performance_monitor.config import REAL_TIME_INTERVAL
from performance_monitor.db import get_session
from performance_monitor.ethernet_and_fiber_channel.model import (
    Ethernet,
    FiberChannel,
    RawEthernetData,
    RawFiberChannelData,
)

KILOBYTE_TO_MEGABYTE: int = 1_000
BYTE_TO_MEGABYTE: int = 1_000_000


async def get_fiber_channel_targets() -> list[str]:
    fc_path = Path("/sys/kernel/scst_tgt/targets/qla2x00t/")
    return [fl.name for fl in fc_path.iterdir() if fl.is_dir()]


async def read_file_content(file_path: Path) -> str:
    async with aiofiles.open(file_path, mode="r") as f:
        return (await f.read()).strip()


async def get_target_pm_data(wwn: str) -> RawFiberChannelData:
    target_path = Path(f"/sys/kernel/scst_tgt/targets/qla2x00t/{wwn}/")
    return RawFiberChannelData(
        time=datetime.now(),
        write_bandwidth=int(await read_file_content(target_path / "write_io_count_kb")),
        read_bandwidth=int(await read_file_content(target_path / "read_io_count_kb")),
        write_count=int(await read_file_content(target_path / "write_cmd_count")),
        read_count=int(await read_file_content(target_path / "read_cmd_count")),
    )


async def fiber_channel_and_ethernet_job() -> NoReturn:
    previous_raw_fiber_channel_data = await get_all_raw_fiber_channel_data()
    previous_raw_ethernet_data = await get_all_ethernet_data()
    while True:
        print(f"collector_fiber_channel_and_etherne started to collect data at {datetime.now()}")
        now = time()
        (previous_raw_fiber_channel_data, previous_raw_ethernet_data), _ = await asyncio.gather(
            insert_pm_data_to_db(previous_raw_fiber_channel_data, previous_raw_ethernet_data),
            asyncio.sleep(REAL_TIME_INTERVAL),
        )
        print(f"collector_fiber_channel_and_etherne finished in {time() - now} seconds at {datetime.now()}")


async def insert_pm_data_to_db(
    previous_raw_fiber_channel_data: dict[str, RawFiberChannelData],
    previous_raw_ethernet_data: dict[str, RawEthernetData],
) -> tuple[dict[str, RawFiberChannelData], dict[str, RawEthernetData]]:
    current_raw_fc_data = await get_all_raw_fiber_channel_data()
    current_raw_ethernet_data = await get_all_ethernet_data()

    with get_session() as session:
        for wwn, fc_data in current_raw_fc_data.items():
            session.add(await get_fiber_channel(wwn, fc_data, previous_raw_fiber_channel_data[wwn]))
        for port_name, ethernet_data in current_raw_ethernet_data.items():
            session.add(await get_ethernet(port_name, ethernet_data, previous_raw_ethernet_data[port_name]))
        session.commit()

    return current_raw_fc_data, current_raw_ethernet_data


async def get_all_raw_fiber_channel_data() -> dict[str, RawFiberChannelData]:
    return {wwn: await get_target_pm_data(wwn) for wwn in await get_fiber_channel_targets()}


async def get_all_ethernet_data() -> dict[str, RawEthernetData]:
    interfaces: dict[str, snetio] = psutil.net_io_counters(pernic=True)
    now = datetime.now()
    return {
        port_name: RawEthernetData(
            time=now,
            bytes_sent=stats.bytes_sent,
            bytes_recv=stats.bytes_recv,
            packets_sent=stats.packets_sent,
            packets_recv=stats.packets_recv,
        )
        for port_name, stats in interfaces.items()
        if port_name != "lo"  # skip loop back
    }


async def get_fiber_channel(wwn: str, d2: RawFiberChannelData, d1: RawFiberChannelData) -> FiberChannel:
    diff = (d2.time - d1.time).total_seconds()
    fc: FiberChannel
    # in some cases the connection is lost and the data is reset
    # in this case we will not calculate the difference
    # and we will just return the current data
    # because the previous data is 0 and the difference will be negative
    if (
        d2.write_bandwidth >= d1.write_bandwidth
        or d2.read_bandwidth >= d1.read_bandwidth
        or d2.write_count >= d1.write_count
        or d2.read_count >= d1.read_count
    ):
        fc = FiberChannel(
            name=wwn,
            time=d2.time,
            write_bandwidth=(d2.write_bandwidth - d1.write_bandwidth) / diff / KILOBYTE_TO_MEGABYTE,
            read_bandwidth=(d2.read_bandwidth - d1.read_bandwidth) / diff / KILOBYTE_TO_MEGABYTE,
            read_iops=(d2.write_count - d1.write_count) / diff,
            write_iops=(d2.read_count - d1.read_count) / diff,
            iops=(d2.write_count - d1.write_count + d2.read_count - d1.read_count) / diff,
            bandwidth=(d2.write_bandwidth - d1.write_bandwidth + d2.read_bandwidth - d1.read_bandwidth)
            / diff
            / KILOBYTE_TO_MEGABYTE,
        )
    else:
        fc = FiberChannel(
            name=wwn,
            time=d2.time,
            write_bandwidth=(d2.write_bandwidth) / diff / KILOBYTE_TO_MEGABYTE,
            read_bandwidth=(d2.read_bandwidth) / diff / KILOBYTE_TO_MEGABYTE,
            read_iops=(d2.write_count) / diff,
            write_iops=(d2.read_count) / diff,
            iops=(d2.write_count + d2.read_count) / diff,
            bandwidth=(d2.write_bandwidth + d2.read_bandwidth) / diff / KILOBYTE_TO_MEGABYTE,
        )

    return fc


async def get_ethernet(port_name: str, d2: RawEthernetData, d1: RawEthernetData) -> Ethernet:
    diff = (d2.time - d1.time).total_seconds()
    ethernet: Ethernet
    #
    if (
        d2.bytes_sent >= d1.bytes_sent
        or d2.bytes_recv >= d1.bytes_recv
        or d2.packets_sent >= d1.packets_sent
        or d2.packets_recv >= d1.packets_recv
    ):
        ethernet = Ethernet(
            name=port_name,
            time=d2.time,
            bytes_sent=(d2.bytes_sent - d1.bytes_sent) / diff / BYTE_TO_MEGABYTE,
            bytes_recv=(d2.bytes_recv - d1.bytes_recv) / diff / BYTE_TO_MEGABYTE,
            packets_sent=d2.packets_sent - d1.packets_sent,
            packets_recv=d2.packets_recv - d1.packets_recv,
            bandwidth=(d2.bytes_sent - d1.bytes_sent + d2.bytes_recv - d1.bytes_recv) / diff / BYTE_TO_MEGABYTE,
        )
    else:
        ethernet = Ethernet(
            name=port_name,
            time=d2.time,
            bytes_sent=d2.bytes_sent,
            bytes_recv=d2.bytes_recv,
            packets_sent=d2.packets_sent,
            packets_recv=d2.packets_recv,
            bandwidth=(d2.bytes_sent + d2.bytes_recv) / diff / KILOBYTE_TO_MEGABYTE,
        )

    return ethernet


if __name__ == "__main__":
    asyncio.run(fiber_channel_and_ethernet_job())
