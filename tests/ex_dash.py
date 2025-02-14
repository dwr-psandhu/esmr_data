# %%
from esmr_data import esmr, dash

esmr_csv_file = "Y:/repo_staging_auto/dropbox/esmr/raw/esmr-analytical-export_years-2006-2025_2025-02-04.csv"
df = esmr.read_data_csv(esmr_csv_file)
# %%
data = esmr.ESMR(df)
# %%
ui = dash.ESMRDash(data)
# %%
ui.show()
