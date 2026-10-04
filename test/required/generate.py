from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

rows = list(range(1, 20))
schema = pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("score", pa.int64(), nullable=False),
    pa.field("measure", pa.float64(), nullable=False),
    pa.field("active", pa.bool_(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
])
values = {
    "id": rows,
    "score": [i * 100 for i in rows],
    "measure": [i / 4 for i in rows],
    "active": [i % 2 == 0 for i in rows],
    "name": ["group" + str(i % 3) for i in rows],
}
table = pa.Table.from_pydict(values, schema=schema)
for dictionary in (False, True):
    variant = "dictionary" if dictionary else "plain"
    path = Path(__file__).parent / ("required-" + variant + ".parquet")
    pq.write_table(table, path, compression="NONE", use_dictionary=dictionary,
                   version="1.0", data_page_version="1.0", row_group_size=7,
                   data_page_size=64, write_batch_size=3, write_statistics=False)
    assert pq.read_table(path).to_pydict() == values
