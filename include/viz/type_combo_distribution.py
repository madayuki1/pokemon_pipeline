import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

parquet_path = Path(__file__).parents[2] / 'shared' / 'poke_api' / 'gold' / 'type_combo_distribution.parquet'
output_path = Path(__file__).parents[2] / 'reports'

# Load data
df = pd.read_parquet(parquet_path)
single_type = df[df["type_combo"].str.contains("/")]
duo_type = df[~df["type_combo"].str.contains("/")]

# Aggregate
single_count = (
    single_type.head(10)
    .sort_values('count', ascending=True)
)
duo_count = (
    duo_type.head(10)
    .sort_values('count', ascending=True)
)

# Plot
plt.figure(figsize=(10, 8))
plt.barh(single_count["type_combo"], single_count["count"], color="#87CEFA")

plt.xlabel("Number of Pokémon")
plt.title("Distribution of Pokémon Type Combos")
plt.tight_layout()

plt.savefig(output_path / 'single_type.png')
plt.close()

# Plot
plt.figure(figsize=(10, 8))
plt.barh(duo_count["type_combo"], duo_count["count"], color="#87CEFA")

plt.xlabel("Number of Pokémon")
plt.title("Distribution of Pokémon Type Combos")
plt.tight_layout()

plt.savefig(output_path / 'duo_type.png')
plt.close()
