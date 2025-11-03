import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "Data"
OUTPUT_DIR = BASE_DIR / "outputs"
USA_FILE = DATA_DIR / "USA" / "samexclusionspublicextract-gsa-1626.csv"
UZB_DIR = DATA_DIR / "Uzbekistan"


def process_us_exclusions() -> pd.DataFrame:
    """Transform the SAM exclusions extract into the unified schema."""
    usecols = [
        "Classification",
        "Name",
        "Prefix",
        "First",
        "Middle",
        "Last",
        "Suffix",
        "City",
        "State / Province",
        "Country",
        "DUNS",
        "Exclusion Program",
        "Excluding Agency",
        "Exclusion Type",
        "Active Date",
        "Termination Date",
        "Record Status",
        "SAM Number",
        "CAGE",
        "Creation_Date",
    ]
    df = pd.read_csv(USA_FILE, usecols=usecols, dtype=str)
    df = df.fillna("")

    def build_vendor_name(row: pd.Series) -> str:
        parts = [row.get("Name", "").strip()]
        if not parts[0]:
            parts = [
                row.get("Prefix", "").strip(),
                row.get("First", "").strip(),
                row.get("Middle", "").strip(),
                row.get("Last", "").strip(),
                row.get("Suffix", "").strip(),
            ]
        return " ".join(p for p in parts if p)

    df["vendor_name"] = df.apply(build_vendor_name, axis=1)
    df["record_date"] = pd.to_datetime(
        df["Active Date"].replace("", pd.NA), errors="coerce"
    )
    fallback_creation = pd.to_datetime(
        df["Creation_Date"].replace("", pd.NA), errors="coerce"
    )
    df.loc[df["record_date"].isna(), "record_date"] = fallback_creation
    df["record_date"] = df["record_date"].dt.strftime("%Y-%m-%d")

    df["country"] = "United States"
    df["record_type"] = "exclusion"
    df["record_source"] = "SAM Exclusions Public Extract (GSA)"
    df["record_id"] = df["SAM Number"]
    df["government_identifier"] = df["DUNS"]
    df["value"] = pd.NA
    df["currency"] = pd.NA
    df["notes"] = (
        df["Excluding Agency"].str.strip()
        + " | "
        + df["Exclusion Program"].str.strip()
        + " | "
        + df["Exclusion Type"].str.strip()
    ).str.strip(" |")
    df["source_url"] = "https://open.gsa.gov/api/sam/"

    keep = [
        "country",
        "record_type",
        "record_source",
        "vendor_name",
        "government_identifier",
        "record_id",
        "record_date",
        "value",
        "currency",
        "notes",
        "source_url",
    ]
    return df[keep]


def _find_column(columns, candidates):
    """Return the first column that contains any of the candidate substrings."""
    for candidate in candidates:
        needle = candidate.lower()
        for col in columns:
            if needle in col.lower():
                return col
    return None


def process_uzbek_awards() -> pd.DataFrame:
    """Stack the direct procurement spreadsheets published by Uzbekistan."""
    records = []
    for path in sorted(UZB_DIR.glob("*.csv")):
        dataset_id = path.stem
        try:
            frame = pd.read_excel(path, engine="openpyxl")
        except Exception:
            frame = pd.read_csv(path, encoding="utf-8")
        frame.columns = [str(col).strip() for col in frame.columns]

        col_vendor = _find_column(
            frame.columns,
            [
                "Name of supplier",
                "Supplier of goods",
                "Supplier",
                "Ишлаб чиқарувчи",
            ],
        )
        col_id = _find_column(frame.columns, ["STIR", "TIN", "ИНН", "pinfl"])
        col_contract = _find_column(
            frame.columns, ["Contract number", "Лот/шартнома", "Лот", "Lot"]
        )
        col_date = _find_column(
            frame.columns,
            [
                "Date of conclusion",
                "Дата договора",
                "Date of contract",
                "Date of registration",
            ],
        )
        col_amount = _find_column(
            frame.columns,
            ["Contract amount", "Amount", "amount of money", "Price"],
        )
        col_currency = _find_column(frame.columns, ["Currency"])
        col_subject = _find_column(
            frame.columns,
            ["subject", "Product name", "товарлар", "Public procurements"],
        )
        col_purchase_type = _find_column(
            frame.columns, ["Purchase type", "type of purchase", "амалга"]
        )
        col_platform = _find_column(frame.columns, ["Platform name"])
        col_funding = _find_column(
            frame.columns, ["Source of funding", "Source of funds", "Source"]
        )

        if not col_vendor:
            continue

        subset = pd.DataFrame()
        subset["vendor_name"] = frame[col_vendor].astype(str).str.strip()
        subset["government_identifier"] = (
            frame[col_id].astype(str).str.strip() if col_id else pd.NA
        )
        subset["record_id"] = (
            frame[col_contract].astype(str).str.strip() if col_contract else pd.NA
        )
        if col_date:
            subset["record_date"] = pd.to_datetime(
                frame[col_date], errors="coerce"
            ).dt.strftime("%Y-%m-%d")
        else:
            subset["record_date"] = pd.NA
        if col_amount:
            subset["value"] = pd.to_numeric(
                frame[col_amount], errors="coerce"
            )
        else:
            subset["value"] = pd.NA
        subset["currency"] = (
            frame[col_currency].astype(str).str.strip() if col_currency else pd.NA
        )
        notes_parts = []
        if col_purchase_type:
            notes_parts.append(
                "Purchase type: "
                + frame[col_purchase_type].astype(str).str.strip()
            )
        if col_platform:
            notes_parts.append(
                "Platform: " + frame[col_platform].astype(str).str.strip()
            )
        if col_subject:
            notes_parts.append(
                "Subject: " + frame[col_subject].astype(str).str.strip()
            )
        if col_funding:
            notes_parts.append(
                "Funding source: "
                + frame[col_funding].astype(str).str.strip()
            )
        if notes_parts:
            subset["notes"] = notes_parts[0]
            for extra in notes_parts[1:]:
                subset["notes"] += " | " + extra
        else:
            subset["notes"] = pd.NA

        frame["country"] = "Uzbekistan"
        frame["record_type"] = "contract_award"
        frame["record_source"] = (
            "Uzbekistan Open Data Portal procurement extracts"
        )
        subset["country"] = "Uzbekistan"
        subset["record_type"] = "contract_award"
        subset["record_source"] = (
            "Uzbekistan Open Data Portal procurement extracts"
        )
        subset["source_url"] = f"https://data.egov.uz/datasets/{dataset_id}"

        keep = [
            "country",
            "record_type",
            "record_source",
            "vendor_name",
            "government_identifier",
            "record_id",
            "record_date",
            "value",
            "currency",
            "notes",
            "source_url",
        ]
        records.append(subset[keep])
    if not records:
        return pd.DataFrame(columns=keep)
    return pd.concat(records, ignore_index=True)


def main():
    OUTPUT_DIR.mkdir(exist_ok=True)
    us_df = process_us_exclusions()
    uzb_df = process_uzbek_awards()
    combined = pd.concat([us_df, uzb_df], ignore_index=True)
    for col in combined.select_dtypes(include="object"):
        combined[col] = combined[col].str.strip()
    combined = combined.drop_duplicates()
    combined = combined.sort_values(
        ["country", "record_type", "vendor_name", "record_date", "record_id"],
        na_position="last",
    )
    combined.to_csv(OUTPUT_DIR / "foia_vendor_risk_dataset.csv", index=False)

    codebook_lines = [
        "column,description",
        "country,Country or sovereign body that published the record.",
        "record_type,Either 'exclusion' from SAM (USA) or 'contract_award' from Uzbekistan procurement extracts.",
        "record_source,Human-readable citation of the original publication channel.",
        "vendor_name,Primary organization or individual name as published.",
        "government_identifier,Unique identifier or tax ID provided by the source (DUNS/STIR).",
        "record_id,SAM Number for exclusions or contract number/lot reference for awards.",
        "record_date,Published effective date (Active Date for exclusions; contract signature date for awards) formatted as YYYY-MM-DD where available.",
        "value,Monetary value when provided (contract amount in Uzbek soum).",
        "currency,ISO or literal currency string as published.",
        "notes,Concatenated contextual metadata (agency, program, purchase type, etc.).",
        "source_url,Landing page for the originating dataset or API.",
    ]
    (OUTPUT_DIR / "foia_vendor_risk_codebook.csv").write_text(
        "\n".join(codebook_lines), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
