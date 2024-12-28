import pandas as pd
import requests

# Correct URL for the raw CSV file on GitHub
github_csv_url = (
    "https://raw.githubusercontent.com/VFA23SCM80S/Test_Email/master/LSP_emails.csv"
)

# Step 1: Pull the CSV file from GitHub
response = requests.get(github_csv_url)
if response.status_code == 200:
    with open("LSP_emails.csv", "wb") as file:
        file.write(response.content)
    print("CSV file downloaded successfully.")
else:
    print("Failed to fetch the CSV file from GitHub.")
    exit()

# Step 2: Read the data
# Local CSV (from step 5)
step_5_data = pd.read_csv(
    "Inactive_Network_LSP.csv"
)  # Replace with the actual filename
# GitHub CSV
lsp_emails = pd.read_csv("LSP_emails.csv")

# Step 3: Merge the data on the 'LSP' column
merged_data = pd.merge(step_5_data, lsp_emails, on="LSP", how="outer")

# Step 4: Save or process the merged data
merged_data.to_csv("merged_LSP_data.csv", index=False)
print("Merged data saved to 'merged_LSP_data.csv'.")
