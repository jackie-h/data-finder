"""Run the shared specs from the spec files of data-finder-specs, not from the Python that wrote them, when DATAFINDER_SPECS_FROM_FILES is set."""
import os

if os.environ.get("DATAFINDER_SPECS_FROM_FILES"):
    from datafinder_examples_tests import spec_files

    spec_files.use_files()
