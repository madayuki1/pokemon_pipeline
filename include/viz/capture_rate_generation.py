import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Paths
parquet_path = Path(__file__).parents[2] / 'shared' / 'poke_api' / 'gold' / 'capture_rate_by_generation.parquet'
output_path = Path(__file__).parents[2] / 'reports' / 'grouped_capture_rate.png'

# Load and prep data
df = pd.read_parquet(parquet_path)

# Ensure rarity and generation are sorted properly
df['generation'] = df['generation'].astype(str)  # Treat generations as categories
df = df[df['rarity'].isin(['normal', 'legendary'])]

# Pivot for grouped bar
pivot = df.pivot(index='generation', columns='rarity', values='avg_capture_rate')

# Plot setup
x = np.arange(len(pivot.index))  # the label locations
width = 0.35  # width of the bars

fig, ax = plt.subplots(figsize=(10, 6))

bars1 = ax.bar(x - width/2, pivot['normal'], width, label='Normal', color="#87CEFA")
bars2 = ax.bar(x + width/2, pivot['legendary'], width, label='Legendary', color="#FFB6C1")

# Labels and titles
ax.set_xlabel("Generation")
ax.set_ylabel("Average Capture Rate")
ax.set_title("Capture Rate Comparison: Normal vs Legendary by Generation")
ax.set_xticks(x)
ax.set_xticklabels(pivot.index)
ax.legend()

# Tidy up
plt.tight_layout()
plt.savefig(output_path)
plt.close()
