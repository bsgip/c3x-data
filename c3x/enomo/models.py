"""
A set of classes for modelling the electrical components of the optimisation system.

These model classes use dataclasses with (optional) Pydantic validation.
As part of this process, all attributes that can be set via `add_x` functions may be directly
passed into the constructor. This has a few benefits:

- validation can be called on each attribute so that additional requirements can be enforced
(for example, ensuring that system generation is never positive)

- type hinting can be applied to the class

- pylint will validate the source

- attribute documentation can all sit within the class

The `add_x` attributes will be retained for backwards compatibility, however Pydantic validation
will not be applied to attributes created in this manner.
"""
# Pylint cannot introspect the validator decorator which returns a classmethod
# pylint:disable=no-self-argument

__all__ = [
    'EnergyStorage', 'Inverter', 'Generation', 'Demand',
    'Tariff', 'DemandTariff', 'LocalTariff', 'CapacityPrices',
    'DispatchRequest', 'EnergySystem'
]

import logging
import numpy as np
from typing import List, Union

logger = logging.getLogger(__name__)

# Attempt to use pydantic dataclasses for validation. If not, create
# a do-nothing decorator and use the standard dataclass library
try:
    from pydantic import validator
    from pydantic.dataclasses import dataclass

except ImportError:
    logger.warning(
        "Pydantic not found. Reverting to standard dataclasses - no validation"
        "on class attributes will be performed."
    )
    # Wrap the dataclass with a decorator so that it takes the same form as Pydantic dataclass
    from dataclasses import dataclass as pure_dataclass_decorator

    def dataclass(*args, **kwargs):
        # Remove config option that Pydantic dataclasses use
        kwargs.pop("config", None)
        return pure_dataclass_decorator(*args, **kwargs)

    # Define a do-nothing validator decorator if we can't use Pydantic
    def validator(*args, **kwargs):
        def do_nothing(func):
            return func
        return do_nothing


import functools

# TODO Make this setting configurable
SILENTLY_CONVERT_TARIFF_LISTS = False


class DataClassConfig:
    """
    Pydantic DataClass config, used to allow `np.ndarray` to be used as a 
    type annotation on attributes.
    """

    arbitrary_types_allowed = True


@dataclass(config=DataClassConfig)
class EnergyStorage:
    """A generic `EnergyStorage` system can be charged to store energy for later discharge.

    Attributes:
        max_capacity: Maximum energy capacity of the system.
        depth_of_discharge_limit: Proportion of energy reserved as a lower bound on the 
            state of charge of the system at any given time.
        charging_power_limit: Maximum rate at which the system can be charged
        discharging_power_limit: Maximum rate at which the system can be discharged
        charging_efficiency: Energy efficiency of conversion to stored energy.
        discharging_efficiency: Energy efficiency of conversion from stored energy.
        throughput_cost: Cost to the system of the roundtrip conversion (both charging and
            discharging the system)
        initial_state_of_charge: Initial State of Charge of the storage system.
        final_state_of_charge_min: When set, require that the system finishes with a state of charge
            above the value.
        fixed_dispatch: List of intervals for which the energy dispatch is fixed. For example,
            an electric vehicle disconnected may be fixed to 0.0, or to a negative value
            if being driven during the period.
        stored_energy_value: Value to associate with unused energy stored in the system
            at the end of the optimisation period.

    Raises:
        ValueError: On instantiation, a `ValueError` is raised when passed in attributes
            are not physically realisable.

    """

    max_capacity: float
    depth_of_discharge_limit: float
    charging_power_limit: float
    discharging_power_limit: float
    charging_efficiency: float
    discharging_efficiency: float
    throughput_cost: float
    initial_state_of_charge: float = 0.0
    final_state_of_charge_min: float = None
    fixed_dispatch: List[Union[float, None]] = None
    stored_energy_value: float = None

    @validator("charging_efficiency", "discharging_efficiency")
    def efficiency_between_zero_and_one(cls, v):
        if v < 0 or v > 1:
            raise ValueError("Efficiency must be between 0.0 and 1.0 inclusive")
        return v

    @validator("depth_of_discharge_limit")
    def storage_dod_between_zero_and_one(cls, v):
        if v < 0 or v > 1:
            raise ValueError("Depth of Discharge must be between 0.0 and 1.0 inclusive")
        return v

    @validator("initial_state_of_charge")
    def initial_soc_within_max_capacity(cls, v, values):
        # TODO The default should be None, and when not passed through we should set
        # the initial SoC to the minimum (inclusive of depth of discharge)
        if v < 0 or v > values["max_capacity"]:
            raise ValueError(
                "Initial State of Charge must be between 0.0 and max_capacity"
            )
        return v

    @validator("final_state_of_charge_min")
    def final_soc_within_max_capacity(cls, v, values):
        # TODO The default should be None, and when not passed through we should set
        # the initial SoC to the minimum (inclusive of depth of discharge)
        if v is not None and (v < 0 or v > values["max_capacity"]):
            raise ValueError(
                "Final State of Charge must be between 0.0 and max_capacity"
            )
        return v

    @property
    def capacity(self):
        return self.max_capacity * (1 - self.depth_of_discharge_limit)

    def calc_capacity(self):
        capacity = self.max_capacity
        if 0 <= self.depth_of_discharge_limit <= 1:
            # Assume we have a decimal representation of the dod limit
            capacity *= 1 - self.depth_of_discharge_limit
        elif 1 < self.depth_of_discharge_limit <= 100:
            # Assume we have a percentage representation of the dod limit
            capacity *= 1 - self.depth_of_discharge_limit / 100.0
        else:
            raise ValueError("The DoD limit should be between 0 - 100")

        return capacity


@dataclass(config=DataClassConfig)
class Inverter:
    """
    The `Inverter` model converts between alternating current and direct current produced/consumed
        by attached solar and storage systems.

        Note: absolute units are not specified in these models, as they may be altered to suit
        the overall system. kilo- and mega- units may be used, providing they are used
        consistently across the entire system.

    Attributes:
        charging_power_limit: Maximum rate (in units of power) at which the inverter can convert
            from AC to DC.
        discharging_power_limit: Maximum rate (in units of power) at which the inverter can convert
            from DC to AC.
        charging_efficiency: Energy efficiency of conversion from AC to DC. Between 0 and 1.
        discharging_efficiency: Energy efficiency of conversion from DC to AC. Between 0 and 1.
        charging_reactive_power_limit: Maximum reactive power that can be generated/consumed
            while the inverter is charging.
        discharging_reactive_power_limit: Maximum reactive power that can be generated/consumed
            while the inverter is discharging.
        reactive_charging_efficiency: TODO
        reactive_discharging_efficiency: TODO
    """

    charging_power_limit: float
    discharging_power_limit: float
    charging_efficiency: float
    discharging_efficiency: float
    charging_reactive_power_limit: float
    discharging_reactive_power_limit: float
    reactive_charging_efficiency: float
    reactive_discharging_efficiency: float

    @validator("charging_efficiency", "discharging_efficiency")
    def efficiency_between_zero_and_one(cls, v):
        if v < 0 or v > 1:
            raise ValueError("Efficiency must be between 0.0 and 1.0 inclusive")
        return v

    @validator("charging_power_limit")
    def charge_power_limit_not_negative(cls, v):
        if v < 0:
            raise ValueError("Charging power limit must not be negative")
        return v

    @validator("discharging_power_limit")
    def discharge_power_limit_not_positive(cls, v):
        if v > 0:
            raise ValueError("Discharging power limit must not be positive")
        return v


@dataclass(config=DataClassConfig)
class Generation:
    """The Generation profile describes the amount of net generation of a system for each interval.

    Raises:
        ValueError: Raises `ValueError` where the generation array contains positive values.
            Note: validation is only performed on attributes passed into the constructor, and
            not on the `add_generation_profile` method

    Attributes:
        generation: Array of non-positive generation values (in units of energy)
        peak_rating: The maximum possible generation from this system (in units of power)
    """

    generation: np.ndarray = None
    peak_rating: int = 0  # TODO Fix the default peak rating value

    def add_generation_profile(self, generation):
        self.generation = generation

    @validator("generation")
    def generation_negative(cls, v):
        if v is None:
            return v
        if v[v > 0].size > 0:
            raise ValueError("Generation values must be <= 0.0")
        return v


@dataclass(config=DataClassConfig)
class Demand:
    """The `Demand` object describes the system energy demand for each period (in units of energy).


    Raises:
        ValueError: Instantiation raises a ValueError if the array of demand values
            contains negative values.
            Note: validation is only performed on values passed directly into the class
            constructor, not on the `add_demand_profile` method.

    """

    demand: np.ndarray = None

    def add_demand_profile(self, demand):
        self.demand = demand

    @validator("demand")
    def demand_non_negative(cls, v):
        if v is None:
            return v
        if v[v < 0].size > 0:
            raise ValueError("Demand values must be >= 0.0")
        return v


# For backwards compatibility after re-naming
Load = Demand
PV = Generation


@dataclass(config=DataClassConfig)
class Tariff:
    """
    The `Tariff` object holds import and export tariffs describing the price to 
        be paid or received for energy imported and exported.
    
    Attributes:
        import_tariff: The price that a user would pay to consume energy.
            Positive value indicates the price that is paid by the consumer.
        export_tariff: The price that a user would receive for exported energy.
            Positive value indicates the price that is paid to the exporter.

    Raises:
        ValueError: On instantiation, a ValueError may be raised if tariffs passed into the
            constructor are not of the appropriate form. 
            Note: validation is only performed when tariffs are passed into the constructor.
            Tariffs set by the `add_x_tariff` method will not be validated.

    Returns:
        [type]: [description]
    """

    import_tariff: dict = None
    export_tariff: dict = None

    @validator("import_tariff", "export_tariff")
    def _validate_tariff(cls, v):
        if v is None:  # Need to allow default None for backwards compatibility
            return v
        for key, value in v.items():
            if not isinstance(key, int):
                raise ValueError("Tariff keys must be integers")
            if not isinstance(value, (float, int)):
                raise ValueError("Tariff values must be a number")
        return v

    def add_tariff_profile_import(self, tariff):
        self.import_tariff = tariff

    def add_tariff_profile_export(self, tariff):
        self.export_tariff = tariff


@dataclass(config=DataClassConfig)
class DemandTariff:

    active_periods: dict = None
    cost: float = 0.0
    minimum_demand: float = 0.0

    @validator("active_periods")
    def _validate_active_periods(cls, v):
        if v is None:  # Need to allow default None for backwards compatibility
            return v
        for key, value in v.items():
            if not isinstance(key, int):
                raise ValueError("Tariff keys must be integers")
            if not isinstance(value, (int, np.int64)):
                raise ValueError("Tariff values must be 0 or 1")
        return v


@dataclass(config=DataClassConfig)
class LocalTariff:
    """LocalTariff objects differentiate between the energy and transport tariffs applied to 
    energy generated and consumed locally with energy imported and exported from the remote grid.

    Attributes:
        le_import_tariff: Local Energy Import Tariff. The price that a user would pay to 
            consume locally generated energy.
        le_export_tariff: Local Energy Export Tariff. The price that a user would receive 
            for exported energy that is consumed locally.
        lt_import_tariff: Local Transport Import Tariff. The cost that is borne by the importer of
            energy for the transport of that energy within the local network.
        lt_export_tariff: Local Transport Export Tariff. The cost that is borne for the export
            of energy that is consumed within the local network.
        re_import_tariff: Remote Energy Import Tariff. The price that a user would pay to 
            consume remotely generated energy.
        re_export_tariff: Remote Energy Export Tariff. The price that a user would receive 
            for energy that is exported to the remote network.
        rt_import_tariff: Remote Transport Import Tariff. The cost that is borne by the importer of
            energy for the transport of that energy from the remote network.
        rt_export_tariff: Remote Transport Export Tariff. The cost that is borne for the export
            of energy to the remote network.

    Raises:
        ValueError: On instantiation, a ValueError may be raised if tariffs passed into the
            constructor are not of the appropriate form. 
            Note: validation is only performed when tariffs are passed into the constructor.
            Tariffs set by the `add_x_tariff` method will not be validated.
    """

    le_import_tariff: dict = None
    le_export_tariff: dict = None
    lt_import_tariff: dict = None
    lt_export_tariff: dict = None
    re_import_tariff: dict = None
    re_export_tariff: dict = None
    rt_import_tariff: dict = None
    rt_export_tariff: dict = None

    @validator(
        "le_import_tariff",
        "le_export_tariff",
        "lt_import_tariff",
        "lt_export_tariff",
        "re_import_tariff",
        "re_export_tariff",
        "rt_import_tariff",
        "rt_export_tariff",
    )
    def _validate_tariff(cls, v):
        if v is None:  # Need to allow default None for backwards compatibility
            return v
        for key, value in v.items():
            if not isinstance(key, int):
                raise ValueError("Tariff keys must be integers")
            if not isinstance(value, (float, int)):
                raise ValueError("Tariff values must be a number")
        return v

    def add_local_energy_tariff_profile_import(self, tariff):
        self.le_import_tariff = tariff

    def add_local_energy_tariff_profile_export(self, tariff):
        self.le_export_tariff = tariff

    def add_local_transport_tariff_profile_import(self, tariff):
        self.lt_import_tariff = tariff

    def add_local_transport_tariff_profile_export(self, tariff):
        self.lt_export_tariff = tariff

    def add_remote_energy_tariff_profile_import(self, tariff):
        self.re_import_tariff = tariff

    def add_remote_energy_tariff_profile_export(self, tariff):
        self.re_export_tariff = tariff

    def add_remote_transport_tariff_profile_import(self, tariff):
        self.rt_import_tariff = tariff

    def add_remote_transport_tariff_profile_export(self, tariff):
        self.rt_export_tariff = tariff


@dataclass(config=DataClassConfig)
class CapacityPrices:
    """Capacity prices are paid to a system that is able to respond to events
    with a given power over a given time. These are essentially modelled on
    FCAS contingency markets (combined).

    Optimisation assumes that the actual dispatch into a capacity market is negligible,
    so the price per unit of energy delivered/received is ignored. Capacity is calculated
    based on the delta between the current action and possible action

    Attributes:
        charge_prices: Price paid for availability to charge the system (or curtail export)
        discharge_prices: Price paid for availability to discharge the system. Technically, 
            could also entail artificially curtailing solar in order to be available for dispatch
            (in reality this scenario is unlikely)
        

    Raises:
        ValueError: On instantiation, a ValueError may be raised if prices passed into the
            constructor are not of the appropriate form. 
            Note: validation is only performed when tariffs are passed into the constructor.
            Tariffs set by the `add_x_tariff` method will not be validated.
    """
    charge_prices: dict = None
    discharge_prices: dict = None

    def add_capacity_prices_charge(self, prices):
        self.charge_prices = prices

    def add_capacity_prices_discharge(self, prices):
        self.discharge_prices = prices


@dataclass(config=DataClassConfig)
class DispatchRequest:
    """
    A request for a system to dispatch across a set of intervals

    TODO Dispatch request attributes need to be described appropriately.
    """

    linear_ramp: object = None
    step: object = None
    hold: object = None

    def add_dispatch_request_linear_ramp(self, request):
        self.linear_ramp = request

    def add_dispatch_request_step(self, request):
        self.step = request

    def add_dispatch_request_hold(self, request):
        self.hold = request


@dataclass(config=DataClassConfig)
class EnergySystem:
    """An `EnergySystem` is a combination of an inverter, and optionally a generation
    and energy storage device.

    Attributes:
        energy_storage: An `EnergyStorage` system
        inverter: An instance of `Inverter`
        generation: An instance of a `Generation` system
        is_hybrid: Whether the inverter/generation/storage system is configured in a hybrid setup
        export_limit: Power limit on export. Defaults to None
        demand_tariff: demand tariff to be applied to power (Va) imported,
        stored_energy_value: value to be applied to remaining energy stored beyond the optimisation period
    """

    energy_storage: EnergyStorage = None
    inverter: Inverter = None
    generation: Generation = None
    is_hybrid: bool = False
    export_limit: float = None
    tariff: Tariff = None
    dispatch: DispatchRequest = None
    demand: Demand = None
    capacity_prices: CapacityPrices = None
    demand_tariff: DemandTariff = None

    def add_energy_storage(self, energy_storage):
        self.energy_storage = energy_storage

    def add_inverter(self, inverter):
        self.inverter = inverter

    def add_generation(self, generation):
        self.generation = generation

    def add_demand(self, demand):
        self.demand = demand

    def add_tariff(self, tariff):
        self.tariff = tariff

    def add_local_tariff(self, tariff):
        self.local_tariff = tariff

    def add_dispatch(self, dispatch):
        self.dispatch = dispatch

    def add_capacity_prices(self, capacity_prices):
        self.capacity_prices = capacity_prices