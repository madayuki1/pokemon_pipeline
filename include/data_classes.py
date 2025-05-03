from dataclasses import dataclass
from typing import List

@dataclass
class Pokemon:
    id: int
    name: str
    is_default: bool
    location_area_encounters: str = "-"
    base_experience: int = -1
    height: int = -1
    weight: int = -1
    order: int = -1
    cries: str = "-"
    forms: str = "-"
    abilities: str = "-"
    game_indices: str = "-"
    held_items: str = "-"
    moves: str = "-"
    species: str = "-"
    stats: str = "-"
    types: str = "-"
    sprites: str = "-"

@dataclass
class Type:
    id: int
    name: str
    generation: str = "-"
    move_damage_class: str = "-"
    damage_relations: str = "-"
    game_indices: str = "-"
    moves: str = "-"
    names: str = "-"
    pokemon: str = "-"
    sprites: str = "-"

@dataclass
class Ability:
    id: int
    name: str
    is_main_series: bool
    generation: str = "-"
    names: str = "-"
    pokemon: str = "-"
    effect_entries: str = "-"
    effect_changes: str = "-"
    flavor_text_entries: str = "-"

@dataclass
class Move:
    id: int
    name: str
    accuracy: int = -1
    effect_chance: int = -1
    pp: int = -1
    priority: int = -1
    power: int = -1
    damage_class: str = "-"
    generation: str = "-"
    target: str = "-"
    type: str = "-"
    
@dataclass
class Info:
    count: int
    next: str
    prev: str

@dataclass
class Response:
    info:Info
    results: List[Pokemon]