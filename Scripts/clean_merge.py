import pandas as pd

# === Load CSVs ===
cpi_df = pd.read_csv("data/1810000401_databaseLoadingData.csv")
wages_df = pd.read_csv("data/1410006401_databaseLoadingData.csv")

# === Clean CPI Data ===
cpi_cleaned = cpi_df[
    (cpi_df["Products and product groups"] == "All-items") &
    (cpi_df["GEO"].notna()) &
    (cpi_df["VALUE"].notna())
].copy()

cpi_cleaned["Date"] = pd.to_datetime(cpi_cleaned["REF_DATE"], format="%Y-%m")
cpi_cleaned["Year"] = cpi_cleaned["Date"].dt.year
cpi_cleaned = cpi_cleaned.rename(columns={
    "GEO": "Province",
    "VALUE": "CPI"
})

# Get average CPI per year and province
cpi_yearly = cpi_cleaned.groupby(["Province", "Year"], as_index=False)["CPI"].mean()

# === Clean Wage Data ===
wages_cleaned = wages_df[
    (wages_df["North American Industry Classification System (NAICS)"] == "Total employees, all industries") &
    (wages_df["GEO"].notna()) &
    (wages_df["VALUE"].notna())
].copy()

wages_cleaned = wages_cleaned.rename(columns={
    "REF_DATE": "Year",
    "GEO": "Province",
    "VALUE": "Wage"
})

# === Merge CPI and Wages on Province + Year ===
merged_df = pd.merge(cpi_yearly, wages_cleaned, on=["Province", "Year"], how="inner")

# === Normalize to Index (2010 = 100) ===
def normalize(df, col):
    return df.groupby("Province")[col].transform(lambda x: x / x.iloc[0] * 100)

merged_df["CPI_Index"] = normalize(merged_df, "CPI")
merged_df["Wage_Index"] = normalize(merged_df, "Wage")
merged_df["Affordability_Gap"] = merged_df["Wage_Index"] - merged_df["CPI_Index"]

# === Save merged dataset ===
merged_df.to_csv("data/merged_data.csv", index=False)
