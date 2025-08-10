import asyncio
import json
import re
from datetime import datetime
from statistics import mean
from sqlmodel import delete
from sqlalchemy.exc import NoResultFound

from performance_monitor.db import get_session
from performance_monitor.pool_and_lun.model import LUNData, PoolData
from performance_monitor.common_repo import command_run

KILOBYTE_TO_MEGABYTE = 1000


async def parse_iostat() -> tuple[datetime, dict[str, dict]]:
    try:
        output = await command_run("iostat -Ntxdy 4.5 1")
    except Exception as e:
        if "No such file or directory" in str(e) or "command not found" in str(e):
            raise Exception(
                "iostat command not found. Please install sysstat package:\n  sudo apt install sysstat"
            ) from e
        raise e

    lines = output.split("\n")

    device_data: dict[str, dict] = {}
    header = None
    timestamp = datetime.now()

    for line in lines:
        if timestamp_match := re.search(  # this is regex which matches the timestamp
            r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2} [APM]{2}", line
        ):
            timestamp = datetime.strptime(timestamp_match.group(), "%m/%d/%Y %I:%M:%S %p")

        # Parse device statistics
        elif line.startswith("Device:"):
            header = line.split()
            for device_line in lines[lines.index(line) + 1 :]:
                if device_line.strip() == "":
                    break
                # values = device_line.split()
                device_name, *device_values_in_str = device_line.split()
                values = list(map(float, device_values_in_str))
                new_device_info = dict(zip(header[1:], values))
                device_data[device_name] = new_device_info

    return timestamp, device_data


async def get_pools_with_luns() -> dict[str, list[str]]:
    """
    Returns:
    dict of pools that have luns and list of its luns
    """

    try:
        lvs: list[dict[str, str]] = json.loads(await command_run("lvs --reportformat json"))["report"][0]["lv"]
    except Exception as e:
        if "No such file or directory" in str(e) or "command not found" in str(e):
            raise Exception("lvs command not found. Please install lvm2 package:\n  sudo apt install lvm2") from e
        raise e

    pools: dict[str, list[str]] = {}
    for lv in lvs:
        if not lv["lv_attr"].startswith("t"):
            if lv["vg_name"] not in pools:
                pools[lv["vg_name"]] = [lv["lv_name"]]
            else:
                pools[lv["vg_name"]].append(lv["lv_name"])
    return pools


# CHANGE THIS WHEN WE SUPPORT PERFORMANCE MONITOR FOR REPLICATION POOL
async def remove_replication_pool(pools: dict[str, list[str]]):
    pools.pop("REPLICATIONMETA", None)


async def pool_and_lun_job():
    try:
        timestamp, iostat_information = await parse_iostat()
        pools = await get_pools_with_luns()

        # CHANGE THIS WHEN WE SUPPORT PERFORMANCE MONITOR FOR REPLICATION POOL
        # we only support it now.
        await remove_replication_pool(pools)

        for pool_name in pools:
            lun_data: list[LUNData] = []
            for lun_name in pools[pool_name]:
                # ignore snapshot lun
                if lun_name.endswith("_snp"):
                    continue

                new_lun_data = iostat_information.get(f"{pool_name}-{lun_name}")
                if new_lun_data is None:
                    continue
                raw_lun = LUNData(
                    name=lun_name if pool_name != "RAPIDSTORE" else f"cache@{lun_name}",
                    time=timestamp,
                    read_iops=new_lun_data["r/s"],
                    write_iops=new_lun_data["w/s"],
                    read_bandwidth=new_lun_data["rkB/s"] / KILOBYTE_TO_MEGABYTE,
                    write_bandwidth=new_lun_data["wkB/s"] / KILOBYTE_TO_MEGABYTE,
                    read_latency=new_lun_data["r_await"],
                    write_latency=new_lun_data["w_await"],
                    iops=new_lun_data["r/s"] + new_lun_data["w/s"],
                    bandwidth=(new_lun_data["rkB/s"] + new_lun_data["wkB/s"]) / KILOBYTE_TO_MEGABYTE,
                    latency=mean([new_lun_data["r_await"], new_lun_data["w_await"]]),
                )
                lun_data.append(raw_lun)
            pool_data = PoolData.create_pool_from_luns(lun_data, pool_name, timestamp)
            with get_session() as session:
                session.add_all(lun_data)
                session.add(pool_data)
                session.commit()

    except Exception as e:
        print(str(e))


def clear_lun_performance_data(lun_name: str) -> None:
    with get_session() as session:
        try:
            session.begin()
            statement = delete(LUNData).where(LUNData.name == lun_name)  # type: ignore
            session.exec(statement)  # type: ignore
            session.commit()
            print(f"all data for {lun_name} is deleted")

        except NoResultFound:
            session.rollback()
            print(f"data for {lun_name} not in lun data")


def clear_pool_performance_data(pool_name: str) -> None:
    with get_session() as session:
        try:
            session.begin()
            statement = delete(PoolData).where(PoolData.name == pool_name)  # type: ignore
            session.exec(statement)  # type: ignore
            session.commit()
            print(f"all data for {pool_name} is deleted")

        except NoResultFound:
            session.rollback()
            print(f"data for {pool_name} not in pool data")


def clear_cache_performance_data(cache_name: str) -> None:
    clear_lun_performance_data(f"cache@{cache_name}")
