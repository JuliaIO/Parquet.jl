These files contain five required columns: Int32, Int64, Float64, Boolean, and
UTF-8 string. Nineteen rows are split into row groups of 7, 7, and 5 rows.
The fixtures use Parquet 1.0 data pages, no compression or optional statistics,
and plain or dictionary encoding as indicated by the filename. Boolean values
use plain encoding in both files.

They were generated with PyArrow 16.1.0 using `generate.py`. PyArrow is needed
only to regenerate the fixtures; Julia tests read the committed files directly.
The script also checks every value through PyArrow's independent reader.
