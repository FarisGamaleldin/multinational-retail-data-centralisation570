import yaml
from pathlib import Path

# Define the path to the db_creds.yaml file
yaml_path = Path('db_creds.yaml')  # Adjust if needed

# Test if the file exists
if yaml_path.exists():
    print(f"YAML file found at: {yaml_path.resolve()}")
else:
    print(f"YAML file not found. Ensure it is located at: {yaml_path.resolve()}")

# Try to load the YAML file
try:
    with open(yaml_path, 'r') as file:
        creds = yaml.safe_load(file)
        print("YAML file loaded successfully!")
        print("Credentials:", creds)
except FileNotFoundError:
    print("File not found. Please check the file location.")
except yaml.YAMLError as e:
    print(f"Error parsing YAML file: {e}")