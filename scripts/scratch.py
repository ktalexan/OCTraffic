
from octraffic import OCTraffic

# Initialize the OCTraffic object
ocs = OCTraffic(part = 1, version = 2025.3)

prj_meta = ocs.project_metadata(silent = False)
prj_dirs = ocs.project_directories(silent = False)
cb = ocs.load_cb()

# Define the path to the raw geodatabase
gdb_raw = prj_dirs["agp_gdb_raw"]
gdbmain_raw = prj_dirs["gdbmain_raw"]

prj_dirs["agp_gdb"]
prj_dirs["gdbmain"]


for fc in ["crashes", "parties", "victims", "collisions"]:
    ocs.delete_feature_class(fc, gdb_path = prj_dirs["agp_gdb"], dataset = "raw")

for fc in ["crashes", "parties", "victims", "collisions"]:
    ocs.delete_feature_class(fc, gdb_path = prj_dirs["gdbmain"], dataset = "raw")
