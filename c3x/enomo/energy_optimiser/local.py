from pyomo import environ as en

from c3x.enomo.energy_optimiser import OptimiserObjective
from c3x.enomo.energy_optimiser.base import EnergyOptimiser


class LocalEnergyOptimiser(EnergyOptimiser):

    def __init__(self, interval_duration, number_of_intervals, energy_system, objective):
        self.enforce_local_feasability = True
        self.enforce_battery_feasability = True

        super().__init__(interval_duration, number_of_intervals, energy_system, objective)

        super().optimise()

    def build_model(self):
        super().build_model()
        #### Local Energy Models ####

        # Net import from the grid (without BTM Storage)
        self.model.local_net_import = en.Var(self.model.Time, initialize=self.system_demand_dct)

        # Net export to the grid (without BTM Storage)
        self.model.local_net_export = en.Var(self.model.Time, initialize=self.system_generation_dct)

        # Local consumption (Satisfy local demand from local generation)
        self.model.local_demand_transfer = en.Var(self.model.Time, within=en.NonNegativeReals, initialize=0.0)

        # Local Energy Tariffs
        self.model.le_import_tariff = en.Param(self.model.Time,
                                               initialize=self.energy_system.local_tariff.le_import_tariff)
        self.model.le_export_tariff = en.Param(self.model.Time,
                                               initialize=self.energy_system.local_tariff.le_export_tariff)
        self.model.lt_import_tariff = en.Param(self.model.Time,
                                               initialize=self.energy_system.local_tariff.lt_import_tariff)
        self.model.lt_export_tariff = en.Param(self.model.Time,
                                               initialize=self.energy_system.local_tariff.lt_export_tariff)
        self.model.re_import_tariff = en.Param(self.model.Time,
                                               initialize=self.energy_system.local_tariff.re_import_tariff)
        self.model.re_export_tariff = en.Param(self.model.Time,
                                               initialize=self.energy_system.local_tariff.re_export_tariff)
        self.model.rt_import_tariff = en.Param(self.model.Time,
                                               initialize=self.energy_system.local_tariff.rt_import_tariff)
        self.model.rt_export_tariff = en.Param(self.model.Time,
                                               initialize=self.energy_system.local_tariff.rt_export_tariff)

        #### Local Grid Flows Peak Power ####

        self.model.local_peak_connection_point_import_power = en.Var(within=en.NonNegativeReals)
        self.model.local_peak_connection_point_export_power = en.Var(within=en.NonNegativeReals)

        def local_peak_connection_point_import(model, interval):
            return model.local_peak_connection_point_import_power >= self.model.storage_charge_grid[interval] + \
                   self.model.storage_discharge_grid[interval] + self.model.local_net_import[interval] + \
                   self.model.local_net_export[interval]

        def local_peak_connection_point_export(model, interval):
            return model.local_peak_connection_point_export_power >= -(self.model.storage_charge_grid[interval] + \
                   self.model.storage_discharge_grid[interval] + self.model.local_net_import[interval] + \
                   self.model.local_net_export[interval])

        self.model.local_peak_connection_point_import_constraint = en.Constraint(self.model.Time,
                                                                           rule=local_peak_connection_point_import)
        self.model.local_peak_connection_point_export_constraint = en.Constraint(self.model.Time,
                                                                           rule=local_peak_connection_point_export)

        """
        TODO For reasons I do not understand, the curtailment export constraint with >= inequality
        causes the solver to fail with a `'c_u_x1636_' is not convex` error. This is only a problem
        with the local optimiser.

        I have not investigated fully, but it may be due to differences in constraints or objectives
        compared to the BTM model.

        The current hack is to enforce equality between system_generation and system_generation_max.
        This has not been tested with an actual `export_limit` applied, presumably this would fail to solve.

        """
        def connection_point_export_curtailment(model, interval):
            """
            Allows generally for generation to be curtailed in order to maximise the objective.
            It is assumed that the supplied system_generation_max
            """
            return model.system_generation[interval] == model.system_generation_max[interval]

        self.model.system_generation_curtailment_constraint = en.Constraint(self.model.Time,
            rule=connection_point_export_curtailment
        )

    def _local_import_export_constraints(self):
        # Calculate the customer net energy import
        def local_net_import(model, time_interval):
            return model.local_net_import[time_interval] == model.system_net_demand[time_interval] + \
                   model.storage_discharge_demand[time_interval] - model.local_demand_transfer[time_interval]

        # calculate the customer net energy export
        def local_net_export(model, time_interval):
            return model.local_net_export[time_interval] == model.system_net_generation[time_interval] + \
                   model.storage_charge_generation[time_interval] + model.local_demand_transfer[time_interval]

        # constrain the use of local energy exports
        def local_demand_transfer_export(model, time_interval):
            return model.local_demand_transfer[time_interval] + model.storage_charge_generation[time_interval] <= - \
            model.system_net_generation[time_interval]

        # constrain the use of local energy imports
        def local_demand_transfer_import(model, time_interval):
            return model.storage_discharge_demand[time_interval] - model.local_demand_transfer[time_interval] >= - \
            model.system_net_demand[time_interval]

        # Add the constraints to the optimisation model
        self.model.local_net_import_constraint = en.Constraint(self.model.Time, rule=local_net_import)
        self.model.local_net_export_constraint = en.Constraint(self.model.Time, rule=local_net_export)
        self.model.local_demand_transfer_export_constraint = en.Constraint(self.model.Time,
                                                                           rule=local_demand_transfer_export)
        self.model.local_demand_transfer_import_constraint = en.Constraint(self.model.Time,
                                                                           rule=local_demand_transfer_import)

    def _local_demand_constraints(self):
        # These set of constraints are designed to enforce the battery to satisfy any residual
        # local demand before discharging to the grid.
        if self.enforce_battery_feasability:
            def electrical_feasability_discharge_grid_one(model: en.ConcreteModel,
                                                          time_interval: int):  # TODO these annotations are probably wrong
                """This constraint (combined with `electrical_feasability_discharge_grid_two`)
                enforces the electrical requirement that the battery must satisfy local demand
                before discharging into the grid. It maps between the boolean variable
                `local_demand_satisfied` and a bound on `storage_discharge_grid`.

                `local_demand_satisfed = 1` corresponds to a lower bound on `storage_discharge_grid` of zero.
                I.e. if local demand is not satisfied, it is impossible to discharge into the grid

                `local_demand_satisfied = 0` corresponds to a lower bound of `-bigM` (effectively no lower bound).

                Args:
                    model: Pyomo model
                    time_interval: time interval variable

                Returns:
                    obj: constraint object
                """
                return model.storage_discharge_grid[time_interval] >= -self.model.bigM * model.local_demand_satisfied[
                    time_interval]

            def electrical_feasability_discharge_grid_two(model: en.ConcreteModel, time_interval: int):
                """This constraint maps between a boolean `local_demand_satisfied` and its correspondence
                to `storage_discharge_demand`. Combined with `electrical_feasability_discharge_grid_one`,
                this enforces the electrical requirement that the battery must satisfy local demand
                before discharging into the grid.

                `local_demand_satisfied = 1` corresponds to `storage_discharge_demand` having the net excess generation
                as an upper bound.

                `local_demand_satisfied = 0` corresponds to `storage_discharge_demand` having an upper bound of 0.

                Args:
                    model: Pyomo model
                    time_interval: time interval passed into constraint equation

                Returns:
                    obj: constraint object
                """
                return model.storage_discharge_demand[time_interval] <= -(
                            model.system_demand[time_interval] + model.system_generation[time_interval]) * \
                       model.local_demand_satisfied[time_interval]

            self.model.efdc_one = en.Constraint(self.model.Time, rule=electrical_feasability_discharge_grid_one)
            self.model.efdc_two = en.Constraint(self.model.Time, rule=electrical_feasability_discharge_grid_two)

            def electrical_feasability_charge_grid_one(model: en.ConcreteModel,
                                                       time_interval: int):  # TODO these annotations are probably wrong
                """This constraint (combined with `electrical_feasability_charge_grid_two`)
                enforces the electrical requirement that the battery must charge from local
                generation before charging from the grid. It maps between the boolean variable
                `local_generation_satisfied` and a bound on `storage_charge_grid`.

                `local_generation_satisfied = 1` corresponds to an upper bound on `storage_charge_grid` of `bigM`.


                `local_generation_satisfied = 0` corresponds to an upper bound of `0` .
                I.e. if local generation is not accounted for, it is impossible to charge from the grid.

                Args:
                    model: Pyomo model
                    time_interval: time interval variable

                Returns:
                    obj: constraint object
                """
                return model.storage_charge_grid[time_interval] <= self.model.bigM * model.local_generation_satisfied[
                    time_interval]

            def electrical_feasability_charge_grid_two(model: en.ConcreteModel, time_interval: int):
                """This constraint maps between a boolean `local_generation_satisfied` and its correspondence
                to `storage_charge_generation`. Combined with `electrical_feasability_charge_grid_one`,
                this enforces the electrical requirement that the battery must charge from local excess
                generation before charging from the grid.

                `local_generation_satisfied = 1` corresponds to `storage_charge_generation` having the net excess generation
                as an upper bound.

                `local_generation_satisfied = 0` corresponds to `storage_charge_generation` having an upper bound of 0.

                Args:
                    model: Pyomo model
                    time_interval: time interval passed into constraint equation

                Returns:
                    obj: constraint object
                """
                return model.storage_charge_generation[time_interval] >= -(
                            model.system_demand[time_interval] + model.system_generation[time_interval]) * \
                       model.local_generation_satisfied[time_interval]

            self.model.efcc_one = en.Constraint(self.model.Time, rule=electrical_feasability_charge_grid_one)
            self.model.efcc_two = en.Constraint(self.model.Time, rule=electrical_feasability_charge_grid_two)

    def _local_generation_constraints(self):
        # Additional rules to enforce electrical feasibility
        # (Without these rules, the local generation can preferentially export to the grid
        # before satisfying local demand)
        if self.enforce_local_feasability:
            def import_export_rule_one(model: en.ConcreteModel, time_interval: int):
                """Enforce a lower bound on `local_net_export` of `0` or `-bigM` depending on
                whether `is_local_exporting` is zero or one.

                Args:
                    model (en.ConcreteModel): Pyomo model
                    time_interval (int): time interval passed into constraint

                Returns:
                    obj: constraint object
                """
                return model.local_net_export[time_interval] >= -model.is_local_exporting[
                    time_interval] * self.model.bigM

            def import_export_rule_two(model: en.ConcreteModel, time_interval: int):
                """Enforce an upper bound on `local_net_import` of `0` or `bigM` depending on
                whether `is_local_exporting` is one or zero. Combined with `import_export_rule_one`,
                this enforces that the system can only be exporting or importing locally.

                Args:
                    model (en.ConcreteModel): Pyomo model
                    time_interval (int): time interval passed into constraint

                Returns:
                    obj: constraint object
                """
                return model.local_net_import[time_interval] <= (
                            1 - model.is_local_exporting[time_interval]) * self.model.bigM

            self.model.ie_one = en.Constraint(self.model.Time, rule=import_export_rule_one)
            self.model.ie_two = en.Constraint(self.model.Time, rule=import_export_rule_two)

    def apply_constraints(self):
        super().apply_constraints()
        self._local_import_export_constraints()
        self._local_demand_constraints()
        self._local_generation_constraints()




    def _local_models_cost(self):
        self.objective += sum(
            (self.model.storage_charge_grid[i] * (self.model.re_import_tariff[i] + self.model.rt_import_tariff[i])) +
            (self.model.storage_discharge_grid[i] * (self.model.re_export_tariff[i] - self.model.rt_export_tariff[i])) +
            (self.model.storage_charge_generation[i] * (
                        -self.model.le_export_tariff[i] + self.model.le_import_tariff[i] + self.model.lt_export_tariff[
                    i] + self.model.lt_import_tariff[i])) +
            (self.model.storage_discharge_demand[i] * (
                        self.model.le_export_tariff[i] - self.model.le_import_tariff[i] - self.model.lt_export_tariff[
                    i] - self.model.lt_import_tariff[i])) +
            (self.model.local_net_import[i] * (self.model.re_import_tariff[i] + self.model.rt_import_tariff[i])) +
            (self.model.local_net_export[i] * (self.model.re_export_tariff[i] - self.model.rt_export_tariff[i])) +
            (self.model.local_demand_transfer[i] * (
                        -self.model.le_export_tariff[i] + self.model.le_import_tariff[i] + self.model.lt_export_tariff[
                    i] + self.model.lt_import_tariff[i]))
            for i in self.model.Time)

    def _local_third_party(self):
        self.objective += sum(
            (self.model.storage_charge_grid[i] * (self.model.re_import_tariff[i] + self.model.rt_import_tariff[i])) +
            (self.model.storage_discharge_grid[i] * (self.model.re_export_tariff[i] - self.model.rt_export_tariff[i])) +
            (self.model.storage_charge_generation[i] * (
                        self.model.le_import_tariff[i] + self.model.lt_import_tariff[i])) +
            (self.model.storage_discharge_demand[i] * (self.model.le_export_tariff[i] - self.model.lt_export_tariff[i]))
            for i in self.model.Time)

    def _local_grid_peak_power(self):
        # ToDo - More work is needed to convert this into a demand tariff objective (i.e. a cost etc.)
        self.objective += self.model.local_peak_connection_point_import_power + self.model.local_peak_connection_point_export_power

    def _local_grid_minimiser(self):
        # ToDo - What is this objective function? Quantises the Connection point?
        self.objective += sum((self.model.storage_charge_grid[i] + self.model.storage_discharge_grid[i]
                               + self.model.local_net_import[i] + self.model.local_net_export[i]) *
                              (self.model.storage_charge_grid[i] + self.model.storage_discharge_grid[i]
                               + self.model.local_net_import[i] + self.model.local_net_export[i])
                              for i in self.model.Time) * self.smallM

    def _greedy_generation_charging(self):
        # Preferentially charge from local solar as soon as possible
        # This amounts to minimising the quantity of exported energy in early periods
        self.objective += sum(self.model.local_net_export[i]
                              * 1 / self.number_of_intervals
                              * (i / self.number_of_intervals)
                              for i in self.model.Time)

    def _greedy_demand_discharging(self):
        self.objective += sum(self.model.local_net_import[i]
                              * 1 / self.number_of_intervals
                              * (-i / self.number_of_intervals)
                              for i in self.model.Time)

    def build_objective(self):
        super().build_objective()
        # Build the objective function ready for optimisation

        if OptimiserObjective.LocalModelsCost in self.objectives:
            self._local_models_cost()

        if OptimiserObjective.LocalThirdParty in self.objectives:
            self._local_third_party()

        if OptimiserObjective.LocalGridPeakPower in self.objectives:
            self._local_grid_peak_power()

        if OptimiserObjective.LocalGridMinimiser in self.objectives:
            self._local_grid_minimiser()

        if OptimiserObjective.GreedyGenerationCharging in self.objectives:
            self._greedy_generation_charging()

        if OptimiserObjective.GreedyDemandDischarging in self.objectives:
            self._greedy_demand_discharging()