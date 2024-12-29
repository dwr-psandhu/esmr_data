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
        # map of facilities (averaging the locations for now) #FIXME:
        facility_location_lat_lon = esmr.build_facility_location_lat_lon(data.df)
        self.facility_lat_lon = (
            facility_location_lat_lon.groupby(
                ["facility_name", "location", "location_desc"]
            )
            .mean()
            .reset_index()
        )
        self.facility_lat_lon = self.facility_lat_lon[
            ["facility_name", "latitude", "longitude"]
        ]
        self.map = self.facility_lat_lon.hvplot.points(
            x="longitude",
            y="latitude",
            geo=True,
            tiles="CartoLight",
            tools=["tap"],
            hover_cols=["facility_name"],
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

    @param.depends("selected", "parameters")
    def plot_facility(self):
        index = self.selected
        # Use only the first index in the array
        first_index = index[0]
        print(index)
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
                vars_for_param = l.get_parameter(p).get_variables()
                if len(vars_for_param) > 0:
                    plt_for_param = hv.Overlay(
                        [v.result.hvplot() for v in vars_for_param]
                    )
                    plots.append(plt_for_param.opts(title=f"{l.name}"))
        # bug in layout not being updated by DynamicMap
        # plots[0] = plots[0].opts(title=facility_name)
        # .opts(title=facility_name) # bug in layout not being updated in DynamicMap
        return hv.Layout(plots).cols(1).opts(title=facility_name)

    def __panel__(self):
        return pn.Row(
            pn.Column(
                self.map,
                pn.Row(self.param.parameters, self.param.location_place_type),
            ),
            pn.panel(self.plot_facility, loading_indicator=True),
        )
