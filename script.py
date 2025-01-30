import os
import requests
import csv
import time
import logging
import sys
import pandas as pd
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth

# =========================
# Configuration and Logging
# =========================

# Load environment variables from .env file
load_dotenv()

# Cisco Umbrella API Credentials
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

# Validate environment variables
if not CLIENT_ID or not CLIENT_SECRET:
    print("Error: CLIENT_ID and CLIENT_SECRET must be set in the .env file.")
    sys.exit(1)

# ServiceNow credentials
INSTANCE = os.getenv("SERVICENOW_INSTANCE")
USERNAME = os.getenv("SERVICENOW_USERNAME")
PASSWORD = os.getenv("SERVICENOW_PASSWORD")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("umbrella_networks_and_identities.log"),
        logging.StreamHandler(sys.stdout),
    ],
)

# API URLs
UMBRELLA_AUTH_URL = "https://api.umbrella.com/auth/v2/token"
UMBRELLA_NETWORKS_URL = "https://api.umbrella.com/deployments/v2/networks"
TOP_IDENTITY_URL = "https://api.umbrella.com/reports/v2/top-identities"
SERVICENOW_URL = f"https://{INSTANCE}/api/now/table/cmn_location"

# =========================
# Cisco Umbrella API Functions
# =========================


def get_umbrella_access_token(client_id, client_secret):
    logging.info("Authenticating with Cisco Umbrella API to obtain access token...")
    try:
        response = requests.post(
            UMBRELLA_AUTH_URL,
            auth=HTTPBasicAuth(client_id, client_secret),
            data={"grant_type": "client_credentials"},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
        response.raise_for_status()
        token_data = response.json()
        access_token = token_data.get("access_token")
        if not access_token:
            logging.error("Access token not found in the authentication response.")
            sys.exit(1)
        logging.info("Successfully obtained access token.")
        return access_token
    except requests.exceptions.RequestException as err:
        logging.error(f"Request exception during authentication: {err}")
        sys.exit(1)


def list_umbrella_networks(access_token, output_file, per_page=100, max_records=10000):
    logging.info("Retrieving list of networks from Cisco Umbrella...")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    all_networks = []
    total_fetched = 0
    current_page = 1

    while True:
        params = {"limit": per_page, "page": current_page}

        try:
            response = requests.get(
                UMBRELLA_NETWORKS_URL, headers=headers, params=params, timeout=10
            )
            if response.status_code == 429:  # Too Many Requests
                logging.warning("Rate limit reached. Retrying after a delay...")
                time.sleep(5)
                continue

            response.raise_for_status()
            response_data = response.json()

            for network in response_data:
                all_networks.append(
                    {
                        "Network Name": network.get("name", "N/A"),
                        "IP Address": network.get("ipAddress", "N/A"),
                    }
                )

            fetched = len(response_data)
            total_fetched += fetched
            logging.info(
                f"Fetched {fetched} records from page {current_page}. Total fetched: {total_fetched}"
            )

            if fetched < per_page or total_fetched >= max_records:
                logging.info(
                    "No more records to fetch or maximum records limit reached."
                )
                break

            current_page += 1
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from Cisco Umbrella: {e}")
            break

    # Export to CSV
    if all_networks:
        logging.info(f"Exporting {len(all_networks)} networks to {output_file}...")
        export_to_csv(all_networks, output_file)
    else:
        logging.info("No network data to export.")


def fetch_top_identities(access_token, output_file, retries=3, delay=5):
    logging.info("Fetching top identities...")
    identities_data = []
    offset = 0

    for attempt in range(retries):
        try:
            while True:
                query_params = {
                    "from": "-1days",
                    "to": "now",
                    "limit": 5000,
                    "offset": offset,
                }
                response = requests.get(
                    TOP_IDENTITY_URL,
                    headers={"Authorization": f"Bearer {access_token}"},
                    params=query_params,
                )

                if response.status_code == 200:
                    data = response.json().get("data", [])
                    if data:
                        identities_data.extend(data)
                        offset += 5000
                        logging.info(f"Fetched {len(data)} identities.")
                    else:
                        break
                else:
                    logging.error(
                        f"Failed to fetch top identities: {response.status_code}"
                    )
                    break

            # Write to CSV
            if identities_data:
                logging.info(
                    f"Exporting {len(identities_data)} identities to {output_file}..."
                )
                with open(output_file, mode="w", newline="") as file:
                    writer = csv.DictWriter(file, fieldnames=["identity_label"])
                    writer.writeheader()
                    for identity in identities_data:
                        writer.writerow(
                            {"identity_label": identity["identity"].get("label", "N/A")}
                        )
            else:
                logging.info("No identities data to export.")
            break  # Exit retry loop if successful

        except requests.exceptions.RequestException as err:
            logging.error(f"Error fetching top identities: {err}")
            if attempt < retries - 1:
                logging.warning(f"Retrying... ({attempt + 1}/{retries})")
                time.sleep(delay)
            else:
                logging.error("Max retries reached. Could not fetch top identities.")


def export_to_csv(data, filename):
    try:
        with open(filename, mode="w", newline="", encoding="utf-8") as csvfile:
            if not data:
                logging.warning("No data available to write to CSV.")
                return
            fieldnames = list(data[0].keys())
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:
                writer.writerow(row)
        logging.info(f"Data successfully exported to {filename}.")
    except IOError as e:
        logging.error(f"I/O error while writing to CSV file: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while writing to CSV file: {e}")


def compare_and_filter_networks():
    with open("umbrella_networks.csv", mode="r", newline="") as network_file:
        reader = csv.DictReader(network_file)
        network_names = {row["Network Name"] for row in reader}

    with open("top_identities.csv", mode="r", newline="") as identity_file:
        reader = csv.DictReader(identity_file)
        active_identity_labels = {row["identity_label"] for row in reader}

    # Identify inactive networks
    inactive_networks = []
    with open("umbrella_networks.csv", mode="r", newline="") as network_file:
        reader = csv.DictReader(network_file)
        for row in reader:
            if row["Network Name"] not in active_identity_labels:
                inactive_networks.append(row)

    # Export inactive networks to CSV
    if inactive_networks:
        export_to_csv(inactive_networks, "inactive_networks.csv")
        logging.info(f"Inactive networks have been written to 'inactive_networks.csv'.")
    else:
        logging.info("No inactive networks found.")


def fetch_snow_data():
    logging.info("Fetching ServiceNow data...")
    params = {"sysparm_query": "", "sysparm_limit": "1000000000"}
    try:
        response = requests.get(
            SERVICENOW_URL,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            params=params,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        data = response.json().get("result", [])
        df = pd.DataFrame(data)
        required_columns = ["u_marsha", "u_lsp"]
        df = (
            df[required_columns]
            .dropna(subset=required_columns)
            .replace("", pd.NA)
            .dropna(subset=required_columns)
        )
        df = df[df["u_lsp"] != "Other"]

        df.to_csv("snow_data.csv", index=False)
        logging.info(f"ServiceNow data has been saved to 'snow_data.csv'.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from ServiceNow: {e}")


def merge_and_create_lsp():
    logging.info("Merging inactive networks with ServiceNow data...")
    inactive_networks = pd.read_csv("inactive_networks.csv")
    snow_data = pd.read_csv("snow_data.csv")

    # Perform the merge operation
    merged = pd.merge(
        inactive_networks,
        snow_data,
        left_on="Network Name",
        right_on="u_marsha",
        how="left",
    )

    # Filter out networks where u_lsp is NaN
    merged = merged[merged["u_lsp"].notna()]

    # Save the filtered result to a new CSV
    output_file_path = "Inactive_Network_LSP.csv"
    merged.to_csv(output_file_path, index=False)

    full_path = os.path.abspath(output_file_path)
    logging.info(f"Inactive networks with LSP have been written to: {full_path}")
    print(f"File created: {full_path}")  # Print the full path for external capture


# def download_lsp_emails():
#     github_csv_url = "https://raw.githubusercontent.com/VFA23SCM80S/TestttttCisc/master/LSP_emails.csv"

#     try:
#         response = requests.get(github_csv_url)
#         if response.status_code == 200:
#             with open("LSP_emails.csv", "wb") as file:
#                 file.write(response.content)
#             print("CSV file downloaded successfully.")
#         else:
#             print("Failed to fetch the CSV file from GitHub.")
#             exit()

#     except requests.exceptions.RequestException as e:
#         print(f"Error downloading the file from GitHub: {e}")
#         exit()
import requests
import os
from dotenv import load_dotenv


def download_lsp_emails():
    """
    Downloads the LSP_emails.csv file from the specified GitHub repository using a personal access token for authentication.
    """
    # Load environment variables from .env file
    load_dotenv()

    # Retrieve the GitHub token from environment variables
    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        raise ValueError(
            "GitHub token not found. Please set the GITHUB_TOKEN environment variable."
        )

    # GitHub repository details
    github_csv_url = "https://git.marriott.com/Network-DevOps/Mind.Cisco-Umbrella-Notification/blob/main/LSP_Contact/LSP_emails.csv"
    # github_csv_url = "https://raw.githubusercontent.com/VFA23SCM80S/TestttttCisc/master/LSP_emails.csv"
    local_filename = "LSP_emails.csv"

    # Set up headers for authentication
    headers = {"Authorization": f"token {github_token}"}

    try:
        # Fetch the CSV file from GitHub
        response = requests.get(github_csv_url, headers=headers)
        response.raise_for_status()  # Raise an HTTPError for bad responses (4xx or 5xx)

        # Write the content to a local file
        with open(local_filename, "wb") as file:
            file.write(response.content)
        print(f"CSV file downloaded successfully and saved as {local_filename}")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.ConnectionError as conn_err:
        print(f"Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        print(f"Timeout error occurred: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        print(f"An error occurred: {req_err}")
    except Exception as err:
        print(f"An unexpected error occurred: {err}")


def merge_and_clean_data():
    # Step 1: Read the merged LSP data and LSP email data
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

    # Convert 'u_lsp' and 'LSP' columns to string for consistency
    step_5_data["u_lsp"] = step_5_data["u_lsp"].astype(str)
    lsp_emails["LSP"] = lsp_emails["LSP"].astype(str)

    # Step 2: Merge the data on the 'u_lsp' and 'LSP' columns
    merged_data = pd.merge(
        step_5_data, lsp_emails, left_on="u_lsp", right_on="LSP", how="outer"
    )

    # Step 3: Drop rows with missing values (NaN) from the merged data
    merged_data = merged_data.dropna()

    # Step 4: Drop duplicate columns if 'u_marsha' and 'Network Name' or 'u_lsp' and 'LSP' are identical
    if "u_marsha" in merged_data.columns and "Network Name" in merged_data.columns:
        if merged_data["u_marsha"].equals(merged_data["Network Name"]):
            merged_data.drop(
                "u_marsha", axis=1, inplace=True
            )  # Drop u_marsha if identical
            logging.info(
                "Dropped 'u_marsha' column as it is identical to 'Network Name'."
            )

    if "u_lsp" in merged_data.columns and "LSP" in merged_data.columns:
        if (merged_data["u_lsp"].notna() & merged_data["LSP"].notna()).all() and (
            merged_data["u_lsp"] == merged_data["LSP"]
        ).all():
            merged_data.drop(
                "u_lsp", axis=1, inplace=True
            )  # Drop u_lsp if identical to LSP
            logging.info("Dropped 'u_lsp' column as it is identical to 'LSP'.")

    # Step 5: Save the cleaned merged data to a new CSV
    output_file_path = "merged_LSP_data.csv"
    merged_data.to_csv(output_file_path, index=False)

    full_path = os.path.abspath(output_file_path)
    logging.info(f"Merged and cleaned data saved to: {full_path}")
    print(f"File created: {full_path}")  # Print the full path for external capture


# =========================
# Main Execution Flow
# =========================


def main():
    access_token = get_umbrella_access_token(CLIENT_ID, CLIENT_SECRET)

    # # Step 1: Retrieve umbrella networks and export to CSV
    # list_umbrella_networks(access_token, "umbrella_networks.csv")

    # # Step 2: Retrieve top identities and export to CSV with retry mechanism
    # fetch_top_identities(access_token, "top_identities.csv")

    # # Step 3: Fetch ServiceNow data and store in CSV
    # fetch_snow_data()

    # # Step 4: Compare and filter networks
    # compare_and_filter_networks()

    # # Step 5: Merge inactive networks with ServiceNow data and create LSP CSV
    # merge_and_create_lsp()

    # Step 6: Download 'LSP_emails.csv' from GitHub
    download_lsp_emails()

    # # Step 7: Merge and clean data and store it in a variable
    # data = merge_and_clean_data()
    # print("Merged and cleaned data")
    # print("data")
    logging.info("Process completed successfully.")


if __name__ == "__main__":
    main()
