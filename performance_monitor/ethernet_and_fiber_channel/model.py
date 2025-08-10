from datetime import datetime

from sqlmodel import SQLModel

from performance_monitor.common_repo import BasePerformanceModel


class FiberChannel(BasePerformanceModel, table=True):
    read_bandwidth: float
    write_bandwidth: float
    bandwidth: float
    read_iops: float
    write_iops: float
    iops: float

    @staticmethod
    def get_fields_must_be_aggrigated_with_sum():
        return (
            FiberChannel.read_bandwidth,
            FiberChannel.write_bandwidth,
            FiberChannel.bandwidth,
            FiberChannel.read_iops,
            FiberChannel.write_iops,
            FiberChannel.iops,
        )

    @staticmethod
    def get_fields_must_be_aggrigated_with_max():
        return tuple()


class Ethernet(BasePerformanceModel, table=True):
    bytes_sent: float
    bytes_recv: float
    bandwidth: float
    packets_sent: int
    packets_recv: int

    @staticmethod
    def get_fields_must_be_aggrigated_with_sum():
        return (
            Ethernet.bytes_sent,
            Ethernet.bytes_recv,
            Ethernet.bandwidth,
            Ethernet.packets_sent,
            Ethernet.packets_recv,
        )

    @staticmethod
    def get_fields_must_be_aggrigated_with_max():
        return tuple()

    @staticmethod
    def get_conditions_for_total_values():
        return Ethernet.name.like("enp7s0f%")  # type: ignore


class RawFiberChannelData(SQLModel):
    time: datetime
    write_bandwidth: int
    read_bandwidth: int
    write_count: int
    read_count: int


class RawEthernetData(SQLModel):
    time: datetime
    bytes_sent: int
    bytes_recv: int
    packets_sent: int
    packets_recv: int
