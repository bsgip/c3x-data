from pyomo import environ as en

from c3x.enomo.energy_optimiser import OptimiserObjective, minutes_per_hour, fcas_total_duration
from c3x.enomo.energy_optimiser.base import EnergyOptimiser


class BTMEnergyOptimiser(EnergyOptimiser):

    def __init__(self, interval_duration, number_of_intervals, energy_system, objective):
        super().__init__(interval_duration, number_of_intervals, energy_system, objective)

        self.use_piecewise_segments = True  # Defined for a future implementation of linear piecewise segments

        super().optimise()

    def build_model(self):
        #### Behind - the - Meter (BTM) Models ####
        super().build_model()

        # Net import from the grid
        self.model.btm_net_import = en.Var(self.model.Time, initialize=self.system_demand_dct)

        # Net export to the grid
        self.model.btm_net_export = en.Var(self.model.Time, initialize=self.system_generation_dct)

        # The import tariff per kWh
        self.model.btm_import_tariff = en.Param(self.model.Time, initialize=self.energy_system.tariff.import_tariff)
        # The export tariff per kWh
        self.model.btm_export_tariff = en.Param(self.model.Time, initialize=self.energy_system.tariff.export_tariff)

        #### BTM Connection Point Peak Power ####

        self.model.peak_connection_point_import_power = en.Var(within=en.NonNegativeReals)
        self.model.peak_connection_point_export_power = en.Var(within=en.NonNegativeReals, bounds=(None, self.energy_system.export_limit))

        def peak_connection_point_import(model, interval):
            return model.peak_connection_point_import_power >= model.btm_net_import[interval]

        def peak_connection_point_export(model, interval):
            return model.peak_connection_point_export_power >= -model.btm_net_export[interval]



        self.model.peak_connection_point_import_constraint = en.Constraint(self.model.Time,
                                                                           rule=peak_connection_point_import)
        self.model.peak_connection_point_export_constraint = en.Constraint(self.model.Time,
                                                                           rule=peak_connection_point_export)

        def connection_point_export_curtailment(model, interval):
            """
            Allows generally for generation to be curtailed in order to maximise the objective.
            It is assumed that the supplied system_generation_max
            """
            return model.system_generation[interval] >= model.system_generation_max[interval]

        self.model.system_generation_curtailment_constraint = en.Constraint(
            self.model.Time,
            rule=connection_point_export_curtailment
        )

        if self.energy_system.demand_tariff is not None:
            # We need this to provide an objective function calculation where there is a non-zero minimum demand charge
            self.model.demand_periods_bool = en.Param(self.model.Time,
                                                      initialize=self.energy_system.demand_tariff.active_periods)
            self.model.excess_demand = en.Var(
                within=en.NonNegativeReals,
                bounds=(self.energy_system.demand_tariff.minimum_demand, None)
            )
            self.model.demand_charge = en.Param(initialize=self.energy_system.demand_tariff.cost)

            def excess_demand(model, interval):
                """
                Constrain the excess demand variable to be calculated based on the net_import in demand periods
                """
                return model.excess_demand >= (
                        self.model.btm_net_import[interval] * self.model.demand_periods_bool[interval]
                )

            self.model.excess_demand_constraint = en.Constraint(
                self.model.Time,
                rule=excess_demand
            )

        #### Piecewise Linear Segments (To be fully implemented later) ####
        '''if self.use_piecewise_segments:
            # The turning points for the piecewise linear segments
            self.model.turning_point_one_ramp = en.Var(self.model.Time, within=en.Boolean, initialize=0)
            self.model.turning_point_two_ramp = en.Var(self.model.Time, within=en.Boolean, initialize=0)
            lims_one = [None] * (len(net) - 1)  # ToDo - Fix this indexing
            lims_two = [None] * (len(net) - 1)  # ToDo - Fix this indexing

            ind = self.energy_system.dispatch.linear_ramp[0]
            lim = self.energy_system.dispatch.linear_ramp[1]
            for i, l in zip(ind, lim):
                lims_one[i] = l[0]
                lims_two[i] = l[1]

            lim_dct_one = dict(enumerate(lims_one))
            self.model.limits_one = en.Param(self.model.Time, initialize=lim_dct_one)

            lim_dct_two = dict(enumerate(lims_two))
            self.model.limits_two = en.Param(self.model.Time, initialize=lim_dct_two)

            self.model.my_set = en.Set(initialize=ind)
            def B1(m, s):
                return m.limits_one[s] <= m.storage_charge_total[s] + m.storage_discharge_total[s] + self.bigM * (1 - m.turning_point_one_ramp[s])

            def B2(m, s):
                return m.limits_one[s] >= m.storage_charge_total[s] + m.storage_discharge_total[s] - self.bigM * m.turning_point_one_ramp[s]

            self.model.B1 = en.Constraint(self.model.my_set, rule=B1)
            self.model.B2 = en.Constraint(self.model.my_set, rule=B2)

            def B3(m, s):
                return m.limits_two[s] <= m.storage_charge_total[s] + m.storage_discharge_total[s] + self.bigM * (1 - m.turning_point_two_ramp[s])

            def B4(m, s):
                return m.limits_two[s] >= m.storage_charge_total[s] + m.storage_discharge_total[s] - self.bigM * m.turning_point_two_ramp[s]

            self.model.B3 = en.Constraint(self.model.my_set, rule=B3)
            self.model.B4 = en.Constraint(self.model.my_set, rule=B4)'''

    def apply_constraints(self):
        super().apply_constraints()

        self._btm_feasability_constraints()
        self._fcas_constraints()
        self._fixed_dispatch_constraints()
        #
        def net_import_export_feasibility(model: en.ConcreteModel, time_interval: int):
            return model.btm_net_import[time_interval] <= model.is_importing[
                time_interval] * self.model.bigM

        def net_import_export_feasibility_two(model: en.ConcreteModel, time_interval: int):
            return model.btm_net_export[time_interval] >= -(1-model.is_importing[time_interval]) * self.model.bigM

        self.model.system_net_import_export_constraint = en.Constraint(
            self.model.Time,
            rule=net_import_export_feasibility
        )

        self.model.system_net_import_export_constraint_two = en.Constraint(
            self.model.Time,
            rule=net_import_export_feasibility_two
        )


    def _btm_feasability_constraints(self):
        # Enforce the limits of charging the energy storage from locally generated energy
        def storage_generation_charging_behaviour(model, time_interval):
            return model.storage_charge_generation[time_interval] <= -model.system_net_generation[time_interval]

        # Enforce the limits of discharging the energy storage to satisfy local demand
        def storage_demand_discharging_behaviour(model, time_interval):
            return model.storage_discharge_demand[time_interval] >= -model.system_net_demand[time_interval]

        # def system_demand_feasibility(model, time_interval):
        #     """
        #     This constraint prevents the result from having a +ve net_demand
        #     and -ve net_generation in the same interval.
        #     """
        #     return model.system_net_generation[time_interval] >= model.system_generation[time_interval]

        # Add the constraints to the optimisation model
        self.model.generation_charging_behaviour_constraint = en.Constraint(self.model.Time,
                                                                            rule=storage_generation_charging_behaviour)
        self.model.local_discharge_behaviour_constraint = en.Constraint(self.model.Time,
                                                                        rule=storage_demand_discharging_behaviour)

        # self.model.system_net_demand_generation_constraint = en.Constraint(
        #     self.model.Time,
        #     rule=system_demand_feasibility
        # )

        # Calculate the net energy import
        def btm_net_connection_point_import(model, time_interval):
            return model.btm_net_import[time_interval] == model.system_net_demand[time_interval] + \
                   model.storage_charge_grid[time_interval] + model.storage_discharge_demand[time_interval]

        # calculate the net energy export
        def btm_net_connection_point_export(model, time_interval):
            return model.btm_net_export[time_interval] == model.system_net_generation[time_interval] + \
                   model.storage_charge_generation[time_interval] + model.storage_discharge_grid[time_interval]

        # Add the constraints to the optimisation model
        self.model.btm_net_import_constraint = en.Constraint(self.model.Time, rule=btm_net_connection_point_import)
        self.model.btm_net_export_constraint = en.Constraint(self.model.Time, rule=btm_net_connection_point_export)


    def _fixed_dispatch_constraints(self):
        if self.energy_system.energy_storage.fixed_dispatch is not None:
            for i, rate in enumerate(self.energy_system.energy_storage.fixed_dispatch):
                if rate is not None:
                    if rate <= 0.0:
                        self.model.storage_charge_total[i].fix(0.0)
                        self.model.storage_discharge_total[i].fix(rate)
                        # self.model.storage_discharge_total[i].setlb(rate)
                        # self.model.storage_discharge_total[i].setub(rate)
                    else:
                        self.model.storage_charge_total[i].fix(rate)
                        # self.model.storage_charge_total[i].setlb(rate)
                        # self.model.storage_charge_total[i].setub(rate)
                        self.model.storage_discharge_total[i].fix(0.0)


    def _fcas_constraints(self):
        #### Capacity Availability ####

        if OptimiserObjective.CapacityAvailability in self.objectives:  # We can always incorporate these but not optimiser for them.
            # Need to find the max of battery capacity / max power versus set point
            self.model.fcas_storage_discharge_power_is_max = en.Var(self.model.Time, within=en.Boolean, initialize=0)
            self.model.fcas_storage_charge_power_is_max = en.Var(self.model.Time, within=en.Boolean, initialize=0)
            self.model.fcas_discharge_power = en.Var(self.model.Time, initialize=0, bounds=(None, 0))#, domain=en.Integers)
            self.model.fcas_charge_power = en.Var(self.model.Time, initialize=0)#, domain=en.Integers)

            def fcas_max_power_raise_rule_one(model, time_interval):
                # Keep the fcas power below the maximum available power
                return model.fcas_discharge_power[time_interval] >= (
                        self.energy_system.energy_storage.discharging_power_limit -
                        (
                            model.storage_charge_total[time_interval] / model.eta_chg
                            + model.storage_discharge_total[time_interval] * model.eta_dischg
                        ) * minutes_per_hour / self.interval_duration
                    )

            def fcas_max_power_raise_rule_two(model, time_interval):
                # Keep the fcas power below that which can be supplied by the available energy storage
                # This needs to account for the ability of the response to stop charging (which requires no energy)
                allocated_energy = (
                    model.storage_charge_total[time_interval] / model.eta_chg
                    + model.storage_discharge_total[time_interval] * model.eta_dischg
                ) * minutes_per_hour / self.interval_duration
                fcas_energy_required_per_bid_unit = (fcas_total_duration / minutes_per_hour) / self.model.eta_dischg
                return model.fcas_discharge_power[time_interval] >= (
                        -self.model.storage_state_of_charge[time_interval] / (fcas_energy_required_per_bid_unit)
                        -allocated_energy
                        # / (fcas_total_duration / minutes_per_hour) - allocated_energy
                    )

            def fcas_max_power_raise_rule_three(model, time_interval):
                return model.fcas_discharge_power[time_interval] <= self.energy_system.energy_storage.discharging_power_limit - (model.storage_charge_total[time_interval] + model.storage_discharge_total[time_interval]) + self.bigM * (1 - self.model.fcas_storage_discharge_power_is_max[time_interval])

            def fcas_max_power_raise_rule_four(model, time_interval):
                return model.fcas_discharge_power[time_interval] <= -self.model.storage_state_of_charge[time_interval] * self.model.eta_dischg / (
                            fcas_total_duration / minutes_per_hour) + self.bigM * self.model.fcas_storage_discharge_power_is_max[time_interval]

            def fcas_max_power_raise_export_limit(model, time_interval):
                # Ensure that the capacity allocated to FCAS won't exceed the site export limit
                power_conversion = minutes_per_hour / self.interval_duration

                excess_charge_power = (self.model.system_generation[time_interval] + model.storage_charge_total[time_interval] + model.storage_discharge_total[time_interval]) * power_conversion
                return -model.fcas_discharge_power[time_interval] - excess_charge_power <= self.model.export_limit


            self.model.fcas_max_power_raise_rule_one_constraint = en.Constraint(self.model.Time,
                                                                                rule=fcas_max_power_raise_rule_one)
            self.model.fcas_max_power_raise_rule_two_constraint = en.Constraint(self.model.Time,
                                                                                rule=fcas_max_power_raise_rule_two)
            # self.model.fcas_max_power_raise_rule_three_constraint = en.Constraint(self.model.Time,
            #                                                                     rule=fcas_max_power_raise_rule_three)
            # self.model.fcas_max_power_raise_rule_four_constraint = en.Constraint(self.model.Time,
            #                                                                     rule=fcas_max_power_raise_rule_four)


            # TODO Probably a better way of doing this
            if self.energy_system.export_limit is not None:
                self.model.fcas_max_power_raise_export_limit_rule = en.Constraint(
                    self.model.Time,
                    rule=fcas_max_power_raise_export_limit
                )

            def fcas_max_power_lower_rule_one(model, time_interval):
                # Keep the fcas power below the maximum available power

                return model.fcas_charge_power[time_interval] <= (
                        self.energy_system.energy_storage.charging_power_limit -
                        (
                            model.storage_charge_total[time_interval] / model.eta_chg
                            + model.storage_discharge_total[time_interval] * model.eta_dischg
                        ) * minutes_per_hour / self.interval_duration
                    )

            def fcas_max_power_lower_rule_two(model, time_interval):
                # Keep the fcas power below that which can be supplied by the available energy storage
                allocated_energy = (
                    model.storage_charge_total[time_interval] / model.eta_chg
                    + model.storage_discharge_total[time_interval] * model.eta_dischg
                ) * minutes_per_hour / self.interval_duration
                return model.fcas_charge_power[time_interval] <= (
                        self.energy_system.energy_storage.max_capacity - self.model.storage_state_of_charge[time_interval]
                    ) / (self.model.eta_chg * fcas_total_duration / minutes_per_hour) + allocated_energy

            def fcas_max_power_lower_rule_three(model, time_interval):
                return model.fcas_charge_power[
                           time_interval] >= self.energy_system.energy_storage.charging_power_limit - (
                               model.storage_charge_total[time_interval] + model.storage_discharge_total[
                           time_interval]) - self.bigM * (
                               1 - self.model.fcas_storage_charge_power_is_max[time_interval])

            def fcas_max_power_lower_rule_four(model, time_interval):
                return model.fcas_charge_power[time_interval] >= (
                        self.energy_system.energy_storage.max_capacity - self.model.storage_state_of_charge[
                    time_interval]) / (self.model.eta_chg * fcas_total_duration / minutes_per_hour) - self.bigM * \
                       self.model.fcas_storage_charge_power_is_max[time_interval]

            self.model.fcas_max_power_lower_rule_one_constraint = en.Constraint(self.model.Time,
                                                                                rule=fcas_max_power_lower_rule_one)
            self.model.fcas_max_power_lower_rule_two_constraint = en.Constraint(self.model.Time,
                                                                                rule=fcas_max_power_lower_rule_two)
            # self.model.fcas_max_power_lower_rule_three_constraint = en.Constraint(self.model.Time,
            #                                                                       rule=fcas_max_power_lower_rule_three)
            # self.model.fcas_max_power_lower_rule_four_constraint = en.Constraint(self.model.Time,
            #


    def _connection_point_cost(self):
        self.objective += sum(
            self.model.btm_import_tariff[i] * self.model.btm_net_import[i] +  # The cost of purchasing energy
            self.model.btm_export_tariff[i] * self.model.btm_net_export[i]  # The value of selling energy
            for i in self.model.Time)

    def _connection_point_energy(self):
        self.objective += sum((-self.model.btm_net_export[i] + self.model.btm_net_import[i])
                              for i in self.model.Time)

    def _greedy_generation_charging(self):
        # Greedy Generation - Favour charging fully from generation in earlier intervals
        self.objective += sum(-self.model.btm_net_export[i]
                              * 1 / self.number_of_intervals
                              * (1 - i / self.number_of_intervals)
                              for i in self.model.Time)

    def _greedy_demand_discharging(self):
        # Greedy Demand Discharging - Favour satisfying all demand from the storage in earlier intervals
        self.objective += sum(self.model.btm_net_import[i]
                              * 1 / self.number_of_intervals
                              * (1 - i / self.number_of_intervals)
                              for i in self.model.Time)

    def _connection_point_peak_power(self):
        # ToDo - More work is needed to convert this into a demand tariff objective (i.e. a cost etc.)
        self.objective += self.model.peak_connection_point_import_power + self.model.peak_connection_point_export_power

    def _connection_point_quantised_peak(self):
        # ToDo - What is this objective function? Quantises the Connection point?
        self.objective += sum(self.model.btm_net_export[i] * self.model.btm_net_export[i] +
                              self.model.btm_net_import[i] * self.model.btm_net_import[i]
                              for i in self.model.Time)

    def _capacity_availability(self):
        # Implement FCAS style objectives

        # Raise value
        self.objective += sum(
            self.model.fcas_discharge_power[i] * self.energy_system.capacity_prices.discharge_prices[i] for i in
            self.model.Time)

        # Lower value
        self.objective += sum(
            -self.model.fcas_charge_power[i] * self.energy_system.capacity_prices.charge_prices[i] for i in
            self.model.Time)

    def _demand_charges(self):
        self.objective += self.model.excess_demand * self.model.demand_charge

    def build_objective(self):
        super().build_objective()
        # Build the objective function ready for optimisation

        if OptimiserObjective.ConnectionPointCost in self.objectives:
            self._connection_point_cost()


        if OptimiserObjective.ConnectionPointEnergy in self.objectives:
            # The amount of energy crossing the meter boundary
            self._connection_point_energy()

        if OptimiserObjective.GreedyGenerationCharging in self.objectives:
            self._greedy_generation_charging()

        if OptimiserObjective.GreedyDemandDischarging in self.objectives:
            self._greedy_demand_discharging()

        if OptimiserObjective.ConnectionPointPeakPower in self.objectives:
            self._connection_point_peak_power()

        if OptimiserObjective.ConnectionPointQuantisedPeak in self.objectives:
            self._connection_point_quantised_peak()

        if OptimiserObjective.CapacityAvailability in self.objectives:
            self._capacity_availability()

        if OptimiserObjective.DemandCharges in self.objectives:
            self._demand_charges()


        '''if OptimiserObjective.PiecewiseLinear in self.objectives: # ToDo - Fix this implementation to make it complete
            for i in self.energy_system.dispatch.linear_ramp[0]:
                objective += -1 * (self.model.storage_charge_total[i] + self.model.storage_discharge_total[i]) * (
                        1 - self.model.turning_point_two_ramp[i])'''