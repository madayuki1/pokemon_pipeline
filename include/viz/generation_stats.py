import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# File paths
parquet_path = Path(__file__).parents[2] / 'shared' / 'poke_api' / 'gold' / 'generation_stats.parquet'
output_path = Path(__file__).parents[2] / 'reports' / 'generation_stats.png'

# Load data
df = pd.read_parquet(parquet_path)

# Plotting setup
plt.figure(figsize=(12, 8))
stat_cols = [
    "avg_hp",
    "avg_attack",
    "avg_defense",
    "avg_special_attack",
    "avg_special_defense",
    "avg_speed"
]

# Plot each stat as a line
for stat in stat_cols:
    plt.plot(df["generation"], df[stat], marker="o", label=stat.replace("avg_", "").replace("_", " ").title())

# Labels and styles
plt.title("Average Pokémon Stats by Generation", fontsize=16)
plt.xlabel("Generation", fontsize=12)
plt.ylabel("Average Stat Value", fontsize=12)
plt.xticks(df["generation"])  # Ensures all generations are shown
plt.legend(title="Base Stats", bbox_to_anchor=(1.05, 1), loc="upper left")
plt.grid(True, linestyle="--", alpha=0.6)
plt.tight_layout()

# Save
plt.savefig(output_path)
plt.close()
