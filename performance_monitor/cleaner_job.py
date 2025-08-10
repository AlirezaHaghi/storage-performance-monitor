from datetime import datetime, timedelta
from sqlalchemy import delete
from performance_monitor.config import MAX_DAYS_TO_KEEP
from performance_monitor.ethernet_and_fiber_channel.model import Ethernet, FiberChannel
from performance_monitor.pool_and_lun.model import PoolData, LUNData
from performance_monitor.db import get_session


async def clean_old_data():
    with get_session() as session:
        session.exec(delete(Ethernet).where(Ethernet.time < datetime.now() - timedelta(days=MAX_DAYS_TO_KEEP)))  # type: ignore
        session.exec(delete(FiberChannel).where(FiberChannel.time < datetime.now() - timedelta(days=MAX_DAYS_TO_KEEP)))  # type: ignore
        session.exec(delete(PoolData).where(PoolData.time < datetime.now() - timedelta(days=MAX_DAYS_TO_KEEP)))  # type: ignore
        session.exec(delete(LUNData).where(LUNData.time < datetime.now() - timedelta(days=MAX_DAYS_TO_KEEP)))  # type: ignore
        session.commit()


def clean_forward_performance_monitor_data(time: datetime):
    with get_session() as session:
        session.exec(delete(Ethernet).where(Ethernet.time > time))  # type: ignore
        session.exec(delete(FiberChannel).where(FiberChannel.time > time))  # type: ignore
        session.exec(delete(PoolData).where(PoolData.time > time))  # type: ignore
        session.exec(delete(LUNData).where(LUNData.time > time))  # type: ignore
        session.commit()
