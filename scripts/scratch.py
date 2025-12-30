
from octraffic import ocTraffic

# Initialize the OCTraffic object
ocs = ocTraffic(part = 1, version = 2025.3)

prj_meta = ocs.project_metadata(silent = False)
prj_dirs = ocs.project_directories(silent = False)
cb = ocs.load_cb()
