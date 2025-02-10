import simpy
import pandas as pd
import random
from datetime import datetime, timedelta

# Constants
NUM_PLAYERS = 100  # Total number of players to simulate
SIMULATION_TIME = 90  # Total simulation time in days
EVENT_TYPES = ["LevelComplete", "InAppPurchase", "SocialInteraction"]
COUNTRIES = ["USA", "Singapore", "Brazil", "Japan", "Germany", "India"]
DEVICE_TYPES = ["Android", "iOS"]

# Initialize random seed
random.seed(42)

# Helper functions
def generate_event_details(event_type):
    """Generate event details based on event type"""
    if event_type == "LevelComplete":
        return f"Level: {random.randint(1, 10)}"
    elif event_type == "InAppPurchase":
        return f"Amount: {random.uniform(0.99, 50.99):.2f}"
    elif event_type == "SocialInteraction":
        return f"Joined Guild: G{random.randint(100, 999)}"
    else:
        return "-"

# Player session process
def player_session(env, player_id, device, country, events):
    while True:
        # Session start
        start_time = env.now
        events.append({
            "EventID": f"E{10000 + len(events)}",
            "PlayerID": player_id,
            "EventTimestamp": start_time,
            "EventType": "SessionStart",
            "EventDetails": "-",
            "DeviceType": device,
            "Location": country
        })

        # Generate events within a session
        session_duration = random.randint(5 * 60, 2 * 60 * 60)  # Session duration between 5 minutes and 2 hours
        while env.now - start_time < session_duration:
            event_type = random.choice(EVENT_TYPES)
            event_time = env.now
            event_details = generate_event_details(event_type)

            # Add event to the list
            events.append({
                "EventID": f"E{10000 + len(events)}",
                "PlayerID": player_id,
                "EventTimestamp": event_time,
                "EventType": event_type,
                "EventDetails": event_details,
                "DeviceType": device,
                "Location": country
            })

            # Wait for next event
            yield env.timeout(random.randint(10, 600))  # Wait between 10 seconds to 10 minutes

        # Session end
        end_time = env.now
        events.append({
            "EventID": f"E{10000 + len(events)}",
            "PlayerID": player_id,
            "EventTimestamp": end_time,
            "EventType": "SessionEnd",
            "EventDetails": f"Duration: {(end_time - start_time) / 60:.1f} mins",
            "DeviceType": device,
            "Location": country
        })

        # Random delay before the next session starts
        yield env.timeout(random.randint(1 * 60 * 60, 24 * 60 * 60))  # Wait between 1 hour and 24 hours

# Simulation setup
def setup_simulation():
    env = simpy.Environment()
    events = []

    # Create players with random properties
    for i in range(NUM_PLAYERS):
        player_id = f"P{10000 + i}"
        device = random.choice(DEVICE_TYPES)
        country = random.choice(COUNTRIES)
        env.process(player_session(env, player_id, device, country, events))

    # Run the simulation
    env.run(until=SIMULATION_TIME * 24 * 60 * 60)  # Convert days to seconds

    return events

# Run the simulation and generate the event log
event_log = setup_simulation()

# Convert the event log to a DataFrame
df_events = pd.DataFrame(event_log)

# Convert timestamps to datetime format
start_date = datetime(2023, 1, 1)
df_events['EventTimestamp'] = df_events['EventTimestamp'].apply(lambda x: start_date + timedelta(seconds=x))

# Save the DataFrame to a CSV file
df_events.to_csv('game_events.csv', index=False)