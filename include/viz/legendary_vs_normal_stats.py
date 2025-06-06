import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from matplotlib.patches import Patch
import numpy as np

parquet_path = Path(__file__).parents[2] / 'shared' / 'poke_api' / 'gold' / 'legendary_vs_normal_stats.parquet'
output_path = Path(__file__).parents[2] / 'reports' / 'legendary_vs_normal_stats.png'

# Load data
df = pd.read_parquet(parquet_path)

data = (
    df.head(10)
    .groupby('rarity')
)

# Stats to analyze
stats = ["hp", "attack", "defense", "special_attack", "special_defense", "speed"]

# Extract stat values for each rarity group
normal_stats = df[df["rarity"] == "normal"][stats]
legendary_stats = df[df["rarity"] == "legendary"][stats]

# Position setup
x = np.arange(len(stats))  # [0, 1, 2, ..., 5]
width = 0.3  # Controls spacing between normal & legendary boxplots

# Boxplot setup
fig, ax = plt.subplots(figsize=(12, 6))

# Collect boxplot objects for styling
boxes = []

for i, stat in enumerate(stats):
    # Normal
    b1 = ax.boxplot(normal_stats[stat].dropna(),
                    positions=[x[i] - width/2],
                    widths=0.25,
                    patch_artist=True,
                    boxprops=dict(facecolor='#87CEFA', color='black'),
                    medianprops=dict(color='black'),
                    whiskerprops=dict(color='black'),
                    capprops=dict(color='black'),
                    flierprops=dict(markerfacecolor='#87CEFA', markeredgecolor='black'))
    boxes.append(b1)

    # Legendary
    b2 = ax.boxplot(legendary_stats[stat].dropna(),
                    positions=[x[i] + width/2],
                    widths=0.25,
                    patch_artist=True,
                    boxprops=dict(facecolor='#FF69B4', color='black'),
                    medianprops=dict(color='black'),
                    whiskerprops=dict(color='black'),
                    capprops=dict(color='black'),
                    flierprops=dict(markerfacecolor='#FF69B4', markeredgecolor='black'))
    boxes.append(b2)

# Axes and labels
ax.set_xticks(x)
ax.set_xticklabels(stats)
ax.set_ylabel("Stat Value")
ax.set_title("Stat Distributions of Normal vs Legendary Pokémon")

# Legend
legend_handles = [
    Patch(facecolor="#87CEFA", label="Normal"),
    Patch(facecolor="#FF69B4", label="Legendary")
]
ax.legend(handles=legend_handles, title="Rarity")

# Tidy up
plt.tight_layout()
plt.savefig(output_path)
plt.close()