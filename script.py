import pandas as pd
import requests

# Correct URL for the raw CSV file on GitHub
github_csv_url = (
    "https://raw.githubusercontent.com/VFA23SCM80S/TestttttCisc/master/LSP_emails.csv"
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
step_5_data = pd.read_csv("Inactive_Network_LSP.csv")
lsp_emails = pd.read_csv("LSP_emails.csv")

# Print column names for debugging
print("Columns in step_5_data:", step_5_data.columns)
print("Columns in lsp_emails:", lsp_emails.columns)

# Remove any leading/trailing spaces in column names
step_5_data.columns = step_5_data.columns.str.strip()
lsp_emails.columns = lsp_emails.columns.str.strip()

# Ensure 'u_lsp' and 'LSP' columns exist in both DataFrames
if "u_lsp" not in step_5_data.columns or "LSP" not in lsp_emails.columns:
    print("Error: 'u_lsp' column not found in one or both of the files.")
    exit()

# Convert 'u_lsp' and 'LSP' columns to string (if needed) for consistency
step_5_data["u_lsp"] = step_5_data["u_lsp"].astype(str)
lsp_emails["LSP"] = lsp_emails["LSP"].astype(str)

# Step 3: Merge the data on the 'u_lsp' and 'LSP' columns
merged_data = pd.merge(
    step_5_data, lsp_emails, left_on="u_lsp", right_on="LSP", how="outer"
)

# Step 4: Save the merged data to a new CSV (will overwrite the previous one)
merged_data.to_csv("merged_LSP_data.csv", index=False)
print("Merged data saved to 'merged_LSP_data.csv'.")
