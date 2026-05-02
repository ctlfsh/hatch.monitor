import secrets
import sys

key = secrets.token_hex(32)
name = sys.argv[1] if len(sys.argv) > 1 else input("Worker name: ")
print(f"\nKey for '{name}':\n{key}\n")
print(f'Add to keys.json: "{key}": "{name}"')
