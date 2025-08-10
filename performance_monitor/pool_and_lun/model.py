from datetime import datetime
from statistics import mean

from performance_monitor.common_repo import BasePerformanceModel


class LUNData(BasePerformanceModel, table=True):
    read_iops: float
    write_iops: float
    read_bandwidth: float
    write_bandwidth: float
    read_latency: float
    write_latency: float
    iops: float
    bandwidth: float
    latency: float

    @staticmethod
    def get_fields_must_be_aggrigated_with_sum():
        return (
            LUNData.read_iops,
            LUNData.write_iops,
            LUNData.read_bandwidth,
            LUNData.write_bandwidth,
            LUNData.read_latency,
            LUNData.write_latency,
            LUNData.iops,
            LUNData.bandwidth,
            LUNData.latency,
        )

    @staticmethod
    def get_fields_must_be_aggrigated_with_max():
        return tuple()


class PoolData(BasePerformanceModel, table=True):
    read_iops: float
    write_iops: float
    read_bandwidth: float
    write_bandwidth: float
    read_latency: float
    write_latency: float
    iops: float
    bandwidth: float
    latency: float

    @staticmethod
    def create_pool_from_luns(luns: list[LUNData], pool_name: str, time: datetime) -> "PoolData":
        read_iops = []
        write_iops = []
        read_bandwidth = []
        write_bandwidth = []
        read_latency = []
        write_latency = []
        for lun in luns:
            read_iops.append(lun.read_iops)
            write_iops.append(lun.write_iops)
            read_bandwidth.append(lun.read_bandwidth)
            write_bandwidth.append(lun.write_bandwidth)
            read_latency.append(lun.read_latency)
            write_latency.append(lun.write_latency)

        return PoolData(
            name=pool_name,
            time=time,
            read_iops=sum(read_iops),
            write_iops=sum(write_iops),
            read_bandwidth=sum(read_bandwidth),
            write_bandwidth=sum(write_bandwidth),
            read_latency=mean(read_latency),
            write_latency=mean(write_latency),
            iops=sum(read_iops + write_iops),
            bandwidth=sum(read_bandwidth + write_bandwidth),
            latency=mean(read_latency + write_latency),
        )

    @staticmethod
    def get_fields_must_be_aggrigated_with_sum():
        return (
            PoolData.read_iops,
            PoolData.write_iops,
            PoolData.read_bandwidth,
            PoolData.write_bandwidth,
            PoolData.read_latency,
            PoolData.write_latency,
            PoolData.iops,
            PoolData.bandwidth,
            PoolData.latency,
        )

    @staticmethod
    def get_fields_must_be_aggrigated_with_max():
        return tuple()
