"""A set of APs and the tasks written for them."""

from dataclasses import dataclass

from generate_dataset.schemas.ap import AP, Task


@dataclass
class Dataset:
    aps: list[AP]
    # By AP id, for every AP of `aps`.
    tasks: dict[str, list[Task]]
