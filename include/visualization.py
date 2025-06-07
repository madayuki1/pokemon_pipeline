import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from matplotlib.patches import Patch
import numpy as np
import os

def type_combo_distribution_plot(parquet_path:str, output_path:str):

    # Check if parquet path exists
    if not parquet_path.exists():
        os.makedirs(parquet_path, exist_ok=True)
    
    if not output_path.exists():
        os.makedirs(output_path, exist_ok=True)

    # Load data
    filename = 'type_combo_distribution'
    df = pd.read_parquet(parquet_path / f'{filename}.parquet')
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

    plt.savefig(output_path / 'single_type_distribution.png')
    plt.close()

    # Plot
    plt.figure(figsize=(10, 8))
    plt.barh(duo_count["type_combo"], duo_count["count"], color="#87CEFA")

    plt.xlabel("Number of Pokémon")
    plt.title("Distribution of Pokémon Type Combos")
    plt.tight_layout()

    plt.savefig(output_path / 'duo_type_distribution.png')
    plt.close()

def generation_stats_plot(parquet_path:str, output_path:str):

    # Check if parquet path exists
    if not parquet_path.exists():
        os.makedirs(parquet_path, exist_ok=True)
    
    if not output_path.exists():
        os.makedirs(output_path, exist_ok=True)

    # Load data
    filename = 'generation_stats'
    df = pd.read_parquet(parquet_path / f'{filename}.parquet')

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
    plt.savefig(output_path / filename)
    plt.close()

def legendary_vs_normal_stats_plot(parquet_path:str, output_path:str):
    
    # Check if parquet path exists
    if not parquet_path.exists():
        os.makedirs(parquet_path, exist_ok=True)
    
    if not output_path.exists():
        os.makedirs(output_path, exist_ok=True)

    # Load data
    filename = 'legendary_vs_normal_stats'
    df = pd.read_parquet(parquet_path / f'{filename}.parquet')

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
    plt.savefig(output_path / filename)
    plt.close()

def capture_rate_by_generation_plot(parquet_path:str, output_path:str):
    # Check if parquet path exists
    if not parquet_path.exists():
        os.makedirs(parquet_path, exist_ok=True)
    
    if not output_path.exists():
        os.makedirs(output_path, exist_ok=True)

    # Load data
    filename = 'capture_rate_by_generation'
    df = pd.read_parquet(parquet_path / f'{filename}.parquet')

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
    plt.savefig(output_path / filename)
    plt.close()

def move_burst_vs_spam_type_plot(parquet_path:str, output_path:str):
    # Check if parquet path exists
    if not parquet_path.exists():
        os.makedirs(parquet_path, exist_ok=True)
    
    if not output_path.exists():
        os.makedirs(output_path, exist_ok=True)

    # Load data
    filename = 'move_burst_vs_spam_type'
    df = pd.read_parquet(parquet_path / f'{filename}.parquet')

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
    plt.savefig(output_path / filename)
    plt.close()

def ability_frequency_plot(parquet_path:str, output_path:str):
    top_n = 6
    # Check if parquet path exists
    if not parquet_path.exists():
        os.makedirs(parquet_path, exist_ok=True)
    
    if not output_path.exists():
        os.makedirs(output_path, exist_ok=True)

    # Load data
    filename = 'ability_frequency'
    df = pd.read_parquet(parquet_path / f'{filename}.parquet')

    df_sorted = df.sort_values(by="count", ascending=False).head(10)
    top_abilities = df_sorted.head(top_n).copy()

    # Plot
    plt.figure(figsize=(8, 8))
    plt.pie(
        top_abilities["count"],
        labels=top_abilities["ability_name"],
        autopct='%1.1f%%',
        startangle=140,
        wedgeprops={"edgecolor": "white"}
    )
    plt.title("Top Pokemon Abilities")

    # Save
    output_path.mkdir(parents=True, exist_ok=True)
    plt.savefig(output_path / "ability_pie_chart.png")
    plt.close()

