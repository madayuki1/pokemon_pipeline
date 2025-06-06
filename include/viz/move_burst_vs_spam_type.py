import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path

# Paths
parquet_path = Path(__file__).parents[2] / 'shared' / 'poke_api' / 'gold' / 'move_burst_vs_spam_type.parquet'
output_path = Path(__file__).parents[2] / 'reports' / 'move_burst_spam_grouped_bar.png'

# Load
df = pd.read_parquet(parquet_path)

# Sort types alphabetically for consistency
df = df.sort_values("type")

# Positioning
x = np.arange(len(df["type"]))  # x-axis index per type
width = 0.35                    # width of each bar

# Plot
fig, ax = plt.subplots(figsize=(12, 7))

bar1 = ax.bar(x - width/2, df["burst_count"], width, label="Burst Moves", color="#FF6B6B")
bar2 = ax.bar(x + width/2, df["spammable_count"], width, label="Spammable Moves", color="#6BCB77")

# Labels & Styling
ax.set_xlabel("Move Type")
ax.set_ylabel("Number of Moves")
ax.set_title("Burst vs Spammable Moves per Type")
ax.set_xticks(x)
ax.set_xticklabels(df["type"], rotation=45, ha="right")
ax.legend()

plt.tight_layout()
plt.savefig(output_path)
plt.close()
