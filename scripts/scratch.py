
from octraffic import OCTraffic

# Initialize the OCTraffic object
ocs = OCTraffic(part = 1, version = 2025.3)

prj_meta = ocs.project_metadata(silent = False)
prj_dirs = ocs.project_directories(silent = False)
cb = ocs.load_cb()

aprx, workspace = ocs.load_aprx(aprx_path = prj_dirs["agp_aprx"], gdb_path = prj_dirs["agp_gdb"], add_to_map = False)

aprx
workspace
