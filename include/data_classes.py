from dataclasses import dataclass
from typing import List

@dataclass
class Pokemon:
    id: int
    name: str
    is_default: str = "-"
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
class PokemonSpecies:
    id: int
    name: str
    order: int = -1
    base_happiness: int = -1
    capture_rate: int = -1
    color: str = "-"
    evolution_chain: str = "-"
    egg_groups: str = "-"
    evolves_from_species: int = -1
    forms_switchable: str = "-"
    gender_rate: int = -1
    growth_rate: str = "-"
    habitat: str = "-"
    has_gender_differences: str = "-"
    hatch_counter: int = 0
    is_baby: str = "-"
    is_legendary: str = "-"
    is_mythical: str = "-"
    shape: str = "-"
    varieties: str = "-"
    generation: str = "-"

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
    is_main_series: str = "-"
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