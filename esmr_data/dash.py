from . import esmr
import panel as pn
import param
import holoviews as hv
from holoviews import opts, dim
import hvplot.pandas
import numpy as np
import pandas as pd
import os
import warnings

warnings.filterwarnings("ignore")
# viz libs
hv.extension("bokeh")
pn.extension()


class ESMRDash(pn.viewable.Viewer):

    selected = param.List(default=[0], doc="Selected node indices to display in plot")

    parameters = param.ListSelector(
        default=["Flow", "Temperature"],
        objects=[
            "Flow",
            "Temperature",
            "Dissolved Oxygen",
            "Electrical Conductivity @ 25 Deg. C",
        ],
    )

    location_place_type = param.ObjectSelector(
        default="Effluent Monitoring",
        objects=[
            "Effluent Monitoring",
            "Receiving Water Monitoring",
            "Influent Monitoring",
            "Internal Monitoring",
            "Groundwater Monitoring",
            "Recycled Water Monitoring",
            "Biosolids Monitoring",
            "Stormwater Monitoring",
            "Non-point Source Monitoring",
        ],
    )

    def __init__(self, data, **params):
        super().__init__(**params)
        self.data = data  # type esmr.ESMR
        # map of facilities (averaging the locations for now)
        facility_location_lat_lon = esmr.build_facility_id_location_id_lat_lon(data.df)
        self.facility_lat_lon = (
            facility_location_lat_lon.groupby(
                [
                    "facility_place_id",
                    "facility_name",
                    "location_place_id",
                    "location",
                    "location_desc",
                ]
            )
            .mean()
            .reset_index()
        )
        self.facility_lat_lon = self.facility_lat_lon[
            ["facility_name", "location", "location_desc", "latitude", "longitude"]
        ]
        self.map = self.facility_lat_lon.hvplot.points(
            x="longitude",
            y="latitude",
            geo=True,
            tiles="CartoLight",
            tools=["tap"],
            hover_cols=["facility_name", "location"],
            nonselection_color="gray",
            nonselection_alpha=0.5,
            s=50,
        ).opts(
            frame_width=400,
            aspect=1,
            active_tools=["wheel_zoom", "pan", "tap"],
            title="Facilities with Lat/Lon information",
        )
        select_map_pts = hv.streams.Selection1D(source=self.map.Points.I, index=[0])
        select_map_pts.add_subscriber(self.set_selected)

    def set_selected(self, index):
        if index is None or len(index) == 0:
            pass  # keep the previous selections
        else:
            self.selected = index

    @param.depends("selected", "parameters", "location_place_type")
    def plot_facility(self):
        index = self.selected
        # Use only the first index in the array
        first_index = index[0]
        print("Plotting:", index)
        return self.plot_parameters(
            self.data,
            self.facility_lat_lon.iloc[first_index, :].facility_name,
            self.parameters,
            self.location_place_type,
        )

    def plot_parameters(self, data, facility_name, parameters, location_place_type):
        f = data.get_facility(facility_name)
        locations = f.get_locations_of_type(location_place_type)
        plots = []
        for l in locations:
            for p in parameters:
                try:
                    vars_for_param = l.get_parameter(p).get_variables()
                except:
                    print(f"Failed to get {p} for {l}")
                    vars_for_param = []
                if len(vars_for_param) > 0:
                    plt_for_param = hv.Overlay(
                        [
                            v.df.hvplot.line(
                                y="result",
                                hover_cols=[
                                    "units",
                                    "calculated_method",
                                    "analytical_method_code",
                                ],
                                label=f"{v.name}:{v.units}:{v.calculated_method}",
                                ylabel=f"{v.name} ({v.units})",
                            )
                            for v in vars_for_param
                        ]
                    )
                    plots.append(
                        plt_for_param.opts(title=f"{l.name}:{l.location_place_id}")
                    )
        return hv.Layout(plots).cols(1).opts(title=facility_name)

    def save_data_filters(self):
        # TODO: Implement this for filtering criteria
        facility_name = self.facility_lat_lon.iloc[self.selected[0], :].facility_name
        f = self.data.get_facility(facility_name)
        locations = f.get_locations_of_type(self.location_place_type)
        parameters = self.parameters
        return facility_name, self.location_place_type, parameters

    def save_data(self, event):
        self._save_button.loading = True
        index = self.selected
        # Use only the first index in the array
        first_index = index[0]
        print("Save data for index:", first_index)
        facility_name = self.facility_lat_lon.iloc[first_index, :].facility_name
        f = self.data.get_facility(facility_name)
        locations = f.get_locations_of_type(self.location_place_type)
        for l in locations:
            for p in self.parameters:
                try:
                    vars_for_param = l.get_parameter(p).get_variables()
                except:
                    print(f"Failed to get {p} for {l}")
                    vars_for_param = []
                if len(vars_for_param) > 0:
                    for v in vars_for_param:
                        v.df.to_csv(
                            f"{facility_name}_{l.location_place_id}_{p}_{v.calculated_method}_{v.units}.csv"
                        )
        self._save_button.loading = False

    def __panel__(self):
        self._save_button = pn.widgets.Button(
            name="Save Data", button_type="primary", width=100
        )
        self._save_button.on_click(self.save_data)
        self._plot_panel = pn.panel(self.plot_facility, loading_indicator=True)
        return pn.Row(
            pn.Column(
                self.map,
                pn.Row(self.param.parameters, self.param.location_place_type),
                self._save_button,
            ),
            self._plot_panel,
        )
