import os

import numpy as np
from pyomo import environ as en
from pyomo.opt import SolverFactory, TerminationCondition
from pyomo.core import Constraint

from c3x.enomo.energy_optimiser import minutes_per_hour, OptimiserObjective


class EnergyOptimiser(object):
    """
    The `EnergyOptimiser` sets up a Pyomo `ConcreteModel` based on the `EnergySystem` and
    associated costs, computes an objective function based on the desired objective,
    and optimises the system.
    """

    def __init__(self, interval_duration, number_of_intervals, energy_system, objective):
        self.interval_duration = interval_duration  # The duration (in minutes) of each of the intervals being optimised over
        self.number_of_intervals = number_of_intervals
        self.energy_system = energy_system

        # Configure the optimiser through setting appropriate environmental variables.
        # TODO allow this to be dynamically configured, and default to the evironment variables
        self.optimiser_engine = os.environ.get('OPTIMISER_ENGINE', '_cplex_shell')  # ipopt doesn't work with int/bool variables
        self.optimiser_engine_executable = os.environ.get('OPTIMISER_ENGINE_EXECUTABLE')

        self.use_bool_vars = True

        # These values have been arbitrarily chosen
        # A better understanding of the sensitivity of these values may be advantageous
        self.bigM = 5000000
        self.smallM = 0.0001

        self.objectives = objective
        self.build_model()
        self.apply_constraints()
        self.build_objective()


    def build_model(self):
        """
        Create a `pyomo` `ConcreteModel` based on the energy system and related tariff structures.

        The model relates `Param`s (fixed inputs) to the `Var`s to be optimised over.
        Most parameters and variables are indexed across the set of intervals.

        Note: the model sets up binary variables such as `model.is_(dis)charging`.
        These are required to later enforce constraints about the electrical sensibility of
        a solution. The use of these binary variables precludes the use of standard linear solvers,
        such as `ipopt`.
        """

        self.model = en.ConcreteModel()

        # We use RangeSet to create a index for each of the time
        # periods that we will optimise within.
        self.model.Time = en.RangeSet(0, self.number_of_intervals - 1)

        # Configure the initial demand and generation
        system_demand = self.energy_system.demand.demand
        system_generation = self.energy_system.generation.generation
        # and convert the data into the right format for the optimiser objects
        self.system_demand_dct = dict(enumerate(system_demand))
        self.system_generation_dct = dict(enumerate(system_generation))

        #### Initialise the optimisation variables (all indexed by self.model.Time) ####

        # The state of charge of the battery
        self.model.storage_state_of_charge = en.Var(self.model.Time,
                                                    bounds=(0, self.energy_system.energy_storage.capacity),
                                                    initialize=0)

        # The increase in energy storage state of charge at each time step
        self.model.storage_charge_total = en.Var(self.model.Time, initialize=0)

        # The decrease in energy storage state of charge at each time step
        self.model.storage_discharge_total = en.Var(self.model.Time, initialize=0)

        # Increase in battery SoC from the Grid
        self.model.storage_charge_grid = en.Var(self.model.Time,
                                                bounds=(0, self.energy_system.energy_storage.charging_power_limit *
                                                        (self.interval_duration / minutes_per_hour)),
                                                initialize=0)

        # Increase in battery SoC from PV Generation
        self.model.storage_charge_generation = en.Var(self.model.Time,
                                                      bounds=(0, self.energy_system.energy_storage.charging_power_limit *
                                                                 (self.interval_duration / minutes_per_hour)),
                                                      initialize=0)

        # Satisfying local demand from the battery
        self.model.storage_discharge_demand = en.Var(self.model.Time,
                                                     bounds=(None, 0),
                                                     initialize=0)

        # Exporting to the grid from the battery
        self.model.storage_discharge_grid = en.Var(self.model.Time,
                                                   bounds=(None, 0),
                                                   initialize=0)

        #### Boolean variables (again indexed by Time) ####

        # These may not be necessary so provide a binary flag for turning them off

        if self.use_bool_vars:
            # Is the battery charging in a given time interval
            self.model.is_charging = en.Var(self.model.Time, within=en.Boolean)
            # Is the battery discharging in a given time interval
            self.model.is_discharging = en.Var(self.model.Time, within=en.Boolean, initialize=0)

            self.model.local_demand_satisfied = en.Var(self.model.Time, within=en.Boolean, initialize=0)
            self.model.local_generation_satisfied = en.Var(self.model.Time, within=en.Boolean, initialize=0)

            self.model.is_importing = en.Var(self.model.Time, within=en.Boolean)
            self.model.is_net_demand = en.Var(self.model.Time, within=en.Boolean)
            # Is the battery discharging in a given time interval
            self.model.is_local_exporting = en.Var(self.model.Time, within=en.Boolean, initialize=0)


        #### Battery Parameters ####

        # The battery charging efficiency
        self.model.eta_chg = en.Param(initialize=self.energy_system.energy_storage.charging_efficiency)
        # The battery discharging efficiency
        self.model.eta_dischg = en.Param(initialize=self.energy_system.energy_storage.discharging_efficiency)
        # The battery charge power limit
        self.model.charging_limit = en.Param(
            initialize=self.energy_system.energy_storage.charging_power_limit * (self.interval_duration / minutes_per_hour))
        # The battery discharge power limit
        self.model.discharging_limit = en.Param(
            initialize=self.energy_system.energy_storage.discharging_power_limit * (self.interval_duration / minutes_per_hour))
        # The throughput cost for the energy storage
        self.model.throughput_cost = en.Param(initialize=self.energy_system.energy_storage.throughput_cost)

        #### Bias Values ####

        # A small fudge factor for reducing the size of the solution set and
        # achieving a unique optimisation solution
        self.model.scale_func = en.Param(initialize=self.smallM)
        # A bigM value for integer optimisation
        self.model.bigM = en.Param(initialize=self.bigM)

        #### Initial Demand / Generation Profile Parameters ####



        # The local energy consumption
        self.model.system_demand = en.Param(self.model.Time, initialize=self.system_demand_dct)
        # The local energy generation
        self.model.system_generation_max = en.Param(self.model.Time, initialize=self.system_generation_dct)
        self.model.system_generation = en.Var(self.model.Time, initialize=self.system_generation_dct)

        system_net = system_demand + system_generation
        system_net_demand = system_net.copy()
        system_net_demand[system_net_demand < 0.0] = 0.0
        system_net_generation = system_net.copy()
        system_net_generation[system_net_generation > 0.0] = 0.0

        self.model.system_net_demand = en.Var(
            self.model.Time,
            initialize=dict(enumerate(system_net_demand))
        )
        self.model.system_net_generation = en.Var(
            self.model.Time,
            initialize=dict(enumerate(system_net_generation))
        )

        self.model.export_limit = en.Param(initialize=self.energy_system.export_limit)


    def _energy_conservation_constraints(self):
        """
        Enforce conservation of energy within each time interval.
        """
        # Calculate the increased state of charge of the energy storage from the
        # imported energy and locally generated energy. We ensure that the
        # storage charging efficiency is taken into account.
        def storage_charge_behaviour(model, time_interval):
            return model.storage_charge_grid[time_interval] + model.storage_charge_generation[time_interval] \
                   == model.storage_charge_total[time_interval] / model.eta_chg

        # Calculate the decreased state of charge of the energy storage from the
        # exported energy and locally consumed energy. We ensure that the
        # storage discharging efficiency is taken into account.
        def storage_discharge_behaviour(model, time_interval):
            return model.storage_discharge_demand[time_interval] + model.storage_discharge_grid[time_interval] \
                   == model.storage_discharge_total[time_interval] * model.eta_dischg

        # Enforce the charging rate limit
        def storage_charge_rate_limit(model, time_interval):
            return (model.storage_charge_grid[time_interval] + model.storage_charge_generation[
                time_interval]) <= model.charging_limit

        # Enforce the discharge rate limit
        def storage_discharge_rate_limit(model, time_interval):
            if self.energy_system.energy_storage.fixed_dispatch[time_interval] is None:
                return (model.storage_discharge_demand[time_interval] + model.storage_discharge_grid[
                    time_interval]) >= model.discharging_limit
            return Constraint.Skip

        # Add the constraints to the optimisation model
        self.model.storage_charge_behaviour_constraint = en.Constraint(self.model.Time, rule=storage_charge_behaviour)
        self.model.storage_discharge_behaviour_constraint = en.Constraint(self.model.Time,
                                                                          rule=storage_discharge_behaviour)
        self.model.storage_charge_rate_limit_constraint = en.Constraint(self.model.Time, rule=storage_charge_rate_limit)
        self.model.storage_discharge_rate_limit_constraint = en.Constraint(self.model.Time,
                                                                           rule=storage_discharge_rate_limit)

        # Demand/Generation constraints
        def demand_generation_balance(model: en.ConcreteModel, time_interval: int):
            return (model.system_demand[time_interval] + model.system_generation[time_interval]) \
                   == (model.system_net_demand[time_interval] + model.system_net_generation[time_interval])

        self.model.demand_generation_balance_constraint = en.Constraint(
            self.model.Time,
            rule=demand_generation_balance
        )

        def net_generation_demand_feasibility(model: en.ConcreteModel, time_interval: int):
            return model.system_net_demand[time_interval] <= model.is_net_demand[
                time_interval] * self.model.bigM

        def net_generation_demand_feasibility_two(model: en.ConcreteModel, time_interval: int):
            return model.system_net_generation[time_interval] >= -(1-model.is_net_demand[time_interval]) * self.model.bigM

        self.model.system_net_demand_generation_constraint = en.Constraint(
            self.model.Time,
            rule=net_generation_demand_feasibility
        )

        self.model.system_net_generation_demand_constraint_two = en.Constraint(
            self.model.Time,
            rule=net_generation_demand_feasibility_two
        )



    def _storage_soc_constraints(self):
        """
        Constraints that force the system to obey conservation of energy between periods.
        There is also an optional `final_soc` rule that requires the battery to end at
        a minimum state of charge.
        """

        # Calculate the state of charge of the battery in each time interval
        initial_state_of_charge = self.energy_system.energy_storage.initial_state_of_charge
        final_minimum_state_of_charge = self.energy_system.energy_storage.final_state_of_charge_min

        def SOC_rule(model, time_interval):
            if time_interval == 0:
                return model.storage_state_of_charge[time_interval] \
                       == initial_state_of_charge + model.storage_charge_total[time_interval] + \
                       model.storage_discharge_total[
                           time_interval]
            else:
                return model.storage_state_of_charge[time_interval] \
                       == model.storage_state_of_charge[time_interval - 1] + model.storage_charge_total[time_interval] + \
                       model.storage_discharge_total[time_interval]

        self.model.Batt_SOC = en.Constraint(self.model.Time, rule=SOC_rule)

        def final_soc_rule(model):
            return model.storage_state_of_charge[self.number_of_intervals - 1] >= final_minimum_state_of_charge

        if final_minimum_state_of_charge:
            self.model.final_soc = en.Constraint(rule=final_soc_rule)

    def _electrical_feasability_constraints(self):
        """
        A set of constraints that ensure that non-physical solutions cannot be found.
        These rules enforce situations like not being able to charge and discharge a battery
        simultaneously.
        """
        # Use bigM formulation to ensure that the battery is only charging or discharging in each time interval
        if self.use_bool_vars:
            # If the battery is charging then the charge energy is bounded from below by -bigM
            # If the battery is discharging the charge energy is bounded from below by zero
            def bool_cd_rule_one(model, time_interval):
                return model.storage_charge_total[time_interval] >= -self.model.bigM * model.is_charging[time_interval]

            # If the battery is charging then the charge energy is bounded from above by bigM
            # If the battery is discharging the charge energy is bounded from above by zero
            def bool_cd_rule_two(model, time_interval):
                return model.storage_charge_total[time_interval] <= self.model.bigM * (
                            1 - model.is_discharging[time_interval])

            # If the battery is charging then the discharge energy is bounded from above by zero
            # If the battery is discharging the discharge energy is bounded from above by bigM
            def bool_cd_rule_three(model, time_interval):
                return model.storage_discharge_total[time_interval] <= self.model.bigM * model.is_discharging[
                    time_interval]

            # If the battery is charging then the discharge energy is bounded from below by zero
            # If the battery is discharging the discharge energy is bounded from below by -bigM
            def bool_cd_rule_four(model, time_interval):
                return model.storage_discharge_total[time_interval] >= -self.model.bigM * (
                            1 - model.is_charging[time_interval])

            # The battery can only be charging or discharging
            def bool_cd_rule_five(model, time_interval):
                return model.is_charging[time_interval] + model.is_discharging[time_interval] == 1

            # Add the constraints to the optimisation model
            self.model.bcdr_one = en.Constraint(self.model.Time, rule=bool_cd_rule_one)
            self.model.bcdr_two = en.Constraint(self.model.Time, rule=bool_cd_rule_two)
            self.model.bcdr_three = en.Constraint(self.model.Time, rule=bool_cd_rule_three)
            self.model.bcdr_four = en.Constraint(self.model.Time, rule=bool_cd_rule_four)
            self.model.bcdr_five = en.Constraint(self.model.Time, rule=bool_cd_rule_five)

        def system_demand_feasibility(model, time_interval):
            """
            This constraint prevents the result from having a +ve net_demand
            and -ve net_generation in the same interval.
            """
            return model.system_net_generation[time_interval] >= model.system_generation[time_interval]

        self.model.system_net_demand_generation_constraint = en.Constraint(
            self.model.Time,
            rule=system_demand_feasibility
        )

    def apply_constraints(self):
        """
        Applies constraints to the model. These constraints relate either to the conservation of
        energy, the electrical feasibility of a solution, or to the calculation of other
        variables (e.g. FCAS bid constraints).
        """
        self._energy_conservation_constraints()
        self._storage_soc_constraints()
        self._electrical_feasability_constraints()




    def _throughput_cost(self):
        # Throughput cost of using energy storage - we attribute half the cost to charging and half to discharging
        self.objective += sum(self.model.storage_charge_total[i] - self.model.storage_discharge_total[i]
                              for i in self.model.Time) * self.model.throughput_cost / 2.0

    def _throughput(self):
        # Throughput of using energy storage - it mirrors the throughput cost above
        self.objective += sum(self.model.storage_charge_total[i] - self.model.storage_discharge_total[i]
                              for i in self.model.Time) * self.model.scale_func

    def _equal_storage_actions(self):
        # ToDo - Which is the better implementation?
        self.objective += sum((self.model.storage_charge_grid[i] * self.model.storage_charge_grid[i]) +
                              (self.model.storage_charge_generation[i] * self.model.storage_charge_generation[i]) +
                              (self.model.storage_discharge_grid[i] * self.model.storage_discharge_grid[i]) +
                              (self.model.storage_discharge_demand[i] * self.model.storage_discharge_demand[i])
                              for i in self.model.Time) * self.model.scale_func

        '''objective += sum(self.model.storage_charge_total[i] * self.model.storage_charge_total[i] +
                         self.model.storage_discharge_total[i] * self.model.storage_discharge_total[i]
                         for i in self.model.Time) * self.model.scale_func'''

    def _stored_energy_value(self):
        self.objective -= self.model.storage_state_of_charge[
                              self.number_of_intervals - 1] * self.energy_system.energy_storage.stored_energy_value

    def build_objective(self):
        # TODO Now that these have been separated out, we can change this to call dynamically
        # the appropriate objective function, based on the objective name

        # Question: how should it be handled when an optimiser subclass has no appropriate
        # objective function matching the objective? Should this throw an error?

        self.objective = 0

        if OptimiserObjective.ThroughputCost in self.objectives:
            self._throughput_cost()

        if OptimiserObjective.Throughput in self.objectives:
            self._throughput()

        if OptimiserObjective.EqualStorageActions in self.objectives:
            self._equal_storage_actions()

        '''if OptimiserObjective.PiecewiseLinear in self.objectives: # ToDo - Fix this implementation to make it complete
            for i in self.energy_system.dispatch.linear_ramp[0]:
                objective += -1 * (self.model.storage_charge_total[i] + self.model.storage_discharge_total[i]) * (
                        1 - self.model.turning_point_two_ramp[i])'''

        if OptimiserObjective.StoredEnergyValue in self.objectives:
            self._stored_energy_value()



    def optimise(self):
        """
        Calls the associated solver and computes the optimal solution
        based on the given objective.
        """
        def objective_function(model):
            return self.objective

        self.model.total_cost = en.Objective(rule=objective_function, sense=en.minimize)

        # set the path to the solver
        if self.optimiser_engine == 'cplex':
            opt = SolverFactory(self.optimiser_engine, executable=self.optimiser_engine_executable)
        else:
            opt = SolverFactory(self.optimiser_engine)

        # Solve the optimisation
        results = opt.solve(self.model, tee=False)

        # TODO Better handling of solver termination conditions
        if results.solver.termination_condition == TerminationCondition.infeasible:
            raise Exception('Failed to find feasible solution.')


    def values(self, variable_name, decimal_places=3):
        """
        Extract a value from the optimiser result.
        """
        output = np.zeros(self.number_of_intervals)
        var_obj = getattr(self.model, variable_name)
        for index in var_obj:
            try:
                output[index] = round(var_obj[index].value, decimal_places)
            except AttributeError:
                output[index] = round(var_obj[index], decimal_places)
        return output

    def result_dct(self, include_indexed_params=True):
        """Extract the resulting `Var`s (and input `Param`s) as a dictionary

        Args:
            include_indexed_params (bool, optional): Whether to include indexed `Param`s in output. Defaults to True.

        Returns:
            dict: Results dict
        """
        if include_indexed_params:
            component_objects = (en.Var, en.Param)
        else:
            component_objects = en.Var
        dct = {}

        for var_obj in self.model.component_objects(component_objects):
            if var_obj.is_indexed():
                dct[var_obj.name] = var_obj.extract_values()

        return dct

    def result_df(self, include_indexed_params=True):
        """Return result (and optionally indexed `Param`s) as a dataframe

        Args:
            include_indexed_params (bool, optional): Whether to include indexed `Param`s in output. Defaults to True.

        Returns:
            pd.DataFrame: Results dataframe
        """
        import pandas as pd  # TODO Check if pandas is otherwise required and import at head of file
        return pd.DataFrame(self.result_dct(include_indexed_params))