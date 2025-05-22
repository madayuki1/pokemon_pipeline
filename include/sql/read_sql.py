import duckdb
from jinja2 import Template
from pathlib import Path

current_path = Path(__file__).resolve()
root_path = current_path.parents[2]
data_path = root_path / 'shared' / 'poke_api'

# sql_path = (current_path.parents[0] / 'pokemon_types.sql').read_text()
sql_path = (current_path.parents[0] / 'test.sql').read_text()

con = duckdb.connect()
query = Template(sql_path).render(silver_path = str(data_path / 'silver'))
df = con.execute(query).fetchdf()
print(df)