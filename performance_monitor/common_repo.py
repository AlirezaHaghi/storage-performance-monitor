import asyncio
import subprocess
from datetime import datetime, timedelta
from typing import Any, Sequence, Type, TypeVar

from sqlalchemy import TextClause, literal_column, text
from sqlmodel import Field, SQLModel, func, select, desc

from performance_monitor.config import MAX_POINT
from performance_monitor.db import get_async_session

T = TypeVar("T")


class BasePerformanceModel(SQLModel):
    name: str = Field(primary_key=True)
    time: datetime = Field(primary_key=True)

    @staticmethod
    def get_fields_must_be_aggrigated_with_sum() -> tuple[Any, ...]: ...

    @staticmethod
    def get_fields_must_be_aggrigated_with_max() -> tuple[Any, ...]: ...

    @staticmethod
    def get_conditions_for_total_values():
        return True


def refactore_result(name: str, filed_names: tuple[str, ...], results: Sequence[SQLModel]):
    transposed_data = list(zip(*results))
    refactored_result = {name: lst for name, lst in zip(filed_names, transposed_data)}

    return {
        "name": name,
        "data": refactored_result,
    }


def get_time_interval_expr(generation) -> TextClause:
    match generation:
        case 5:
            return text("strftime('%Y-%m-%d', time, '-' || (strftime('%d', time) % 2) || ' days')")  # per 2 days
        case 4:
            return text(
                "strftime('%Y-%m-%d %H:00', time, '-' || (strftime('%H', time) % 4) || ' hours')"
            )  # per 4 hours
        case 3:
            return text("strftime('%Y-%m-%d %H:00', time)")  # per 1 hour
        case 2:
            return text(
                "strftime('%Y-%m-%d %H:%M:00', time, '-' || (strftime('%M', time) % 5) || ' minutes')"
            )  # per 5 minutes
        case 1:
            return text("strftime('%Y-%m-%d %H:%M:00', time)")  # per minute
        case 0:
            return text("strftime('%Y-%m-%d %H:%M:%S', time, '-' || (strftime('%S', time) % 5) || ' seconds')")
        case _:
            raise Exception(f"invalide {generation=}")


def get_filed_name(model_filed) -> str:
    return str(model_filed).rsplit(".")[-1]


# this function is not used because we are ignoring the start and end time
def get_default_start_time_based_on_generation(generation: int) -> datetime:
    match generation:
        case 0:
            return datetime.now() - timedelta(days=2)
        case 1:
            return datetime.now() - timedelta(days=4)
        case 2:
            return datetime.now() - timedelta(weeks=1)
        case 3:
            return datetime.now() - timedelta(weeks=2)
        case 4:
            return datetime.now() - timedelta(weeks=5)
        case 5:
            return datetime.now() - timedelta(weeks=10)
        case _:
            raise Exception(f"invalide {generation=}")


async def check_iostat_availability() -> bool:
    """
    Check if iostat command is available on the system.

    Returns:
        bool: True if iostat is available, False otherwise
    """
    try:
        await command_run("which iostat")
        return True
    except Exception:
        return False


async def command_run(command: str) -> str:
    """
    Execute a shell command asynchronously and return the output.

    Args:
        command: The shell command to execute

    Returns:
        str: The command output (stdout)

    Raises:
        Exception: If the command fails (stderr contains output)
    """
    process = await asyncio.create_subprocess_shell(
        cmd=command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    if stderr:
        raise Exception(stderr.decode())
    return stdout.decode()


def ensure_system_requirements():
    """
    Check if required system commands are available and provide installation instructions if not.
    This function should be called at application startup.
    """
    required_commands = {"iostat": "sysstat", "lvs": "lvm2"}

    missing_commands = []

    for command, package in required_commands.items():
        try:
            result = subprocess.run(["which", command], capture_output=True, text=True, check=False)
            if result.returncode != 0:
                missing_commands.append((command, package))
        except Exception as e:
            print(f"ERROR: Could not check for {command} availability: {e}")
            missing_commands.append((command, package))

    if missing_commands:
        print("ERROR: Required system commands not found!")
        print("This application requires the following packages to be installed:")
        for command, package in missing_commands:
            print(f"  - {package}: provides the {command} command")
        print("\nPlease install them using:")
        print("  sudo apt install " + " ".join(package for _, package in missing_commands))
        print("\nAfter installation, restart the application.")
        exit(1)


async def get_monitoring_data(name: str, model: Type[BasePerformanceModel], generation: int, start, end):
    time_interval_expr = get_time_interval_expr(generation)
    time_interval_clause = literal_column(f"({time_interval_expr})")
    async with get_async_session() as session:
        statement = (
            select(
                time_interval_clause,
                *list(
                    map(
                        lambda value: func.round(func.avg(value), 2),
                        model.get_fields_must_be_aggrigated_with_sum(),
                    )
                ),
                *list(
                    map(
                        lambda value: func.round(func.avg(value), 2),
                        model.get_fields_must_be_aggrigated_with_max(),
                    )
                ),
            )
            .where(
                model.name == name if name else model.get_conditions_for_total_values(),
                model.time >= start,
                model.time <= end,
            )
            .group_by(time_interval_clause)
            .order_by(desc(time_interval_clause))
            .limit(MAX_POINT)
        )
        result_ = await session.execute(statement)
        result = result_.all()[::-1]

    field_names = (
        "time",
        *list(map(get_filed_name, model.get_fields_must_be_aggrigated_with_sum())),
        *list(map(get_filed_name, model.get_fields_must_be_aggrigated_with_max())),
    )
    extended_result = await extend_with_null(
        result,
        generation,
        len(model.get_fields_must_be_aggrigated_with_sum()) + len(model.get_fields_must_be_aggrigated_with_max()),
        time_frame=(start, end),
    )
    refactored_result = refactore_result(name, field_names, extended_result)
    return refactored_result


async def extend_with_null(result, generation, length_of_fields, time_frame) -> list[Any]:
    times = await get_time_series(generation, time_frame=time_frame)
    return [(sample, *((None,) * length_of_fields)) for sample in times[: -len(result)]] + result


async def get_time_series(generation, max_point=MAX_POINT, time_frame=(None, None)):
    time_interval_expr = get_time_interval_expr(generation)
    time_interval_clause = literal_column(f"({time_interval_expr})")
    from performance_monitor.ethernet_and_fiber_channel.model import Ethernet

    async with get_async_session() as session:
        statement = (
            select(time_interval_clause)
            .where(Ethernet.name == "eno1", (Ethernet.time >= time_frame[0]) & (Ethernet.time <= time_frame[1]))
            .order_by(desc(time_interval_clause))
            .limit(max_point)
        )
        result_ = await session.execute(statement)
        result = result_.fetchall()[::-1]
    return [row[0] for row in result]
