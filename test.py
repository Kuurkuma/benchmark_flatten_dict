from memory_profiler import profile, memory_usage
from flatdict import FlatDict
from main import generate_sample_data

match_details = generate_sample_data(100)

@profile
def flatdict_flatten(match_details):
    """Flattens player stats using the flatdict library."""

    player_stats = []
    player_list = match_details.get('home', {}).get('teamsheet', [])
    for player in player_list:
        stats = player.pop('match_stats', {})
        flat_stats = FlatDict(stats, delimiter='_')
        player.update(flat_stats)
        player_stats.append(player)

    return player_stats

flatdict_flatten(match_details)