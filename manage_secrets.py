import sys
import os

# Define the file where secrets will be stored
SECRET_FILE = ".env"

def add_secret(key, value):
    # Append the secret to the .env file
    with open(SECRET_FILE, "a") as f:
        f.write(f"{key.upper()}={value}\n")
    print(f"Successfully added {key.upper()} to {SECRET_FILE}")

def main():
    if len(sys.argv) != 3:
        print("Usage: python manage_secrets.py <KEY> <VALUE>")
        return

    key = sys.argv[1]
    value = sys.argv[2]
    add_secret(key, value)

if __name__ == "__main__":
    main()
