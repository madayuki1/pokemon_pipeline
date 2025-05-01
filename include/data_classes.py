from dataclasses import dataclass, field
from typing import List, Dict, Any

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
    cries: dict = field(default_factory=dict)
    forms: List[dict] = field(default_factory=list)
    abilities: List[dict] = field(default_factory=list)
    game_indices: List[dict] = field(default_factory=list)
    held_items: List[Dict] = field(default_factory=list)
    moves: List[dict] = field(default_factory=list)
    species: dict = field(default_factory=dict)
    stats: List[dict] = field(default_factory=list)
    types: List[str] = field(default_factory=list)
    sprites: dict = field(default_factory=dict)

@dataclass
class Type:
    id: int
    name: str
    generation: dict = field(default_factory=dict)
    move_damage_class: dict = field(default_factory=dict)
    damage_relations: dict = field(default_factory=dict)
    game_indices: List[dict] = field(default_factory=list)
    moves: List[dict] = field(default_factory=list)
    names: List[dict] = field(default_factory=list)
    pokemon: List[dict] = field(default_factory=list)
    sprites: dict = field(default_factory=dict)

@dataclass
class Ability:
    id: int
    name: str
    is_main_series: bool
    generation: dict = field(default_factory=dict)
    names: List[dict] = field(default_factory=list)
    pokemon: List[dict] = field(default_factory=list)
    effect_entries: List[dict] = field(default_factory=list)
    effect_changes: List[dict] = field(default_factory=list)
    flavor_text_entries: List[dict] = field(default_factory=list)
    
@dataclass
class Info:
    count: int
    next: str
    prev: str

@dataclass
class Response:
    info:Info
    results: List[Pokemon]