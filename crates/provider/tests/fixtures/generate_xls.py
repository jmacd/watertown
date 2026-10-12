# SPDX-FileCopyrightText: 2026 Caspar Water Company
#
# SPDX-License-Identifier: Apache-2.0

"""Regenerate synthetic BIFF8 fixtures with Python's xlwt package.

The checked-in .xls files are sufficient for Rust tests; xlwt is not a
build or runtime dependency.
"""

from datetime import datetime
from pathlib import Path

import xlwt


ROOT = Path(__file__).parent
DATE_STYLE = xlwt.easyxf(num_format_str="YYYY-MM-DD HH:MM:SS")


def table(workbook, name):
    sheet = workbook.add_sheet(name)
    # Offset the table to exercise Calamine's populated-range origin.
    for column, value in enumerate(
        ["name", "value", "active", "when", "mixed", "", "late_mixed"], start=1
    ):
        sheet.write(2, column, value)
    for row, values in enumerate(
        [
            ["alpha", 10, True, datetime(2026, 1, 1, 12), 1, None, 1],
            ["beta", 20.5, False, datetime(2026, 1, 2, 12), "text", None, 2],
            ["gamma", 30, None, None, None, None, "last"],
        ],
        start=3,
    ):
        for column, value in enumerate(values, start=1):
            if value is not None:
                sheet.write(
                    row, column, value,
                    DATE_STYLE if isinstance(value, datetime) else xlwt.Style.default_style,
                )


single = xlwt.Workbook()
table(single, "Readings")
single.save(str(ROOT / "single.xls"))

multiple = xlwt.Workbook()
table(multiple, "Readings")
other = multiple.add_sheet("Other Sheet")
other.write(0, 0, "value")
other.write(1, 0, 99)
multiple.add_sheet("Empty")
headers = multiple.add_sheet("HeaderOnly")
headers.write(0, 0, "name")
headers.write(0, 1, "value")
duplicates = multiple.add_sheet("Duplicate")
duplicates.write(0, 0, "same")
duplicates.write(0, 1, "same")
duplicates.write(1, 0, 1)
errors = multiple.add_sheet("Errors")
errors.write(2, 1, "value")
errors.write(3, 1, 1)
errors.row(4).set_cell_error(1, "#DIV/0!")
header_error = multiple.add_sheet("HeaderError")
header_error.row(0).set_cell_error(0, "#REF!")
header_error.write(1, 0, 1)
no_header = multiple.add_sheet("NoHeader")
no_header.write(0, 0, 1)
no_header.write(0, 1, True)
no_header.write(1, 0, 2)
no_header.write(1, 1, False)
multiple.save(str(ROOT / "multiple.xls"))
