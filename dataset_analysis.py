"""
FOIA/Open Data Analysis and Insights Generator
Comprehensive analysis of procurement risk and transparency metrics
"""

import pandas as pd
from pathlib import Path
from datetime import datetime
from collections import Counter

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "outputs"
DATASET_FILE = OUTPUT_DIR / "foia_vendor_risk_dataset.csv"


def load_dataset():
    """Load the unified FOIA dataset"""
    return pd.read_csv(DATASET_FILE)


def generate_summary_statistics(df: pd.DataFrame) -> dict:
    """Calculate comprehensive dataset statistics"""
    
    stats = {
        "total_records": len(df),
        "unique_vendors": df["vendor_name"].nunique(),
        "countries": df["country"].nunique(),
        "date_range_start": pd.to_datetime(df["record_date"], errors='coerce').min() if "record_date" in df.columns else None,
        "date_range_end": pd.to_datetime(df["record_date"], errors='coerce').max() if "record_date" in df.columns else None,
        "us_exclusions": len(df[df["record_type"] == "exclusion"]),
        "uzbek_awards": len(df[df["record_type"] == "contract_award"]),
        "records_with_value": df["value"].notna().sum() if "value" in df.columns else 0,
        "total_contract_value": pd.to_numeric(df["value"], errors='coerce').sum() if "value" in df.columns else 0
    }
    
    return stats


def analyze_exclusions(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze US SAM exclusions patterns"""
    
    exclusions = df[df["record_type"] == "exclusion"].copy()
    
    if exclusions.empty:
        return pd.DataFrame()
    
    # Extract agency and program from notes
    exclusions["agency"] = exclusions["notes"].str.split(" | ").str[0]
    exclusions["program"] = exclusions["notes"].str.split(" | ").str[1]
    exclusions["exclusion_type"] = exclusions["notes"].str.split(" | ").str[2]
    
    # Top excluding agencies
    top_agencies = exclusions["agency"].value_counts().head(10)
    
    # Top exclusion programs
    top_programs = exclusions["program"].value_counts().head(10)
    
    # Exclusion types
    excl_types = exclusions["exclusion_type"].value_counts()
    
    # Time trend
    exclusions["year"] = pd.to_datetime(exclusions["record_date"]).dt.year
    yearly_trends = exclusions.groupby("year").size()
    
    analysis = {
        "total_exclusions": len(exclusions),
        "unique_excluded_vendors": exclusions["vendor_name"].nunique(),
        "top_agencies": top_agencies.to_dict(),
        "top_programs": top_programs.to_dict(),
        "exclusion_types": excl_types.to_dict(),
        "yearly_trends": yearly_trends.to_dict()
    }
    
    return pd.DataFrame([analysis])


def analyze_uzbek_contracts(df: pd.DataFrame) -> pd.DataFrame:
    """Analyze Uzbekistan contract awards patterns"""
    
    contracts = df[df["record_type"] == "contract_award"].copy()
    
    if contracts.empty:
        return pd.DataFrame()
    
    # Contract value analysis
    contracts_with_value = contracts[contracts["value"].notna()]
    
    if not contracts_with_value.empty:
        value_stats = {
            "total_contracts": len(contracts),
            "contracts_with_value": len(contracts_with_value),
            "total_value": contracts_with_value["value"].sum(),
            "mean_value": contracts_with_value["value"].mean(),
            "median_value": contracts_with_value["value"].median(),
            "max_value": contracts_with_value["value"].max(),
            "min_value": contracts_with_value["value"].min()
        }
    else:
        value_stats = {
            "total_contracts": len(contracts),
            "contracts_with_value": 0,
            "total_value": 0,
            "mean_value": 0,
            "median_value": 0,
            "max_value": 0,
            "min_value": 0
        }
    
    # Top vendors by contract count
    top_vendors = contracts["vendor_name"].value_counts().head(20)
    
    # Top vendors by value
    if not contracts_with_value.empty:
        vendor_values = contracts_with_value.groupby("vendor_name")["value"].agg([
            ("total_value", "sum"),
            ("contract_count", "count"),
            ("avg_value", "mean")
        ]).sort_values("total_value", ascending=False).head(20)
    else:
        vendor_values = pd.DataFrame()
    
    return {
        "value_stats": value_stats,
        "top_vendors_by_count": top_vendors.to_dict(),
        "top_vendors_by_value": vendor_values if not vendor_values.empty else None
    }


def cross_reference_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Identify vendors appearing in both US exclusions and Uzbek contracts"""
    
    us_vendors = set(df[df["country"] == "United States"]["vendor_name"].str.upper())
    uzb_vendors = set(df[df["country"] == "Uzbekistan"]["vendor_name"].str.upper())
    
    # Find potential matches (simplified - real analysis needs fuzzy matching)
    common_names = us_vendors.intersection(uzb_vendors)
    
    cross_ref = []
    for name in common_names:
        us_records = df[(df["country"] == "United States") & 
                       (df["vendor_name"].str.upper() == name)]
        uzb_records = df[(df["country"] == "Uzbekistan") & 
                        (df["vendor_name"].str.upper() == name)]
        
        cross_ref.append({
            "vendor_name": name,
            "us_records": len(us_records),
            "uzbek_records": len(uzb_records),
            "note": "Requires manual verification"
        })
    
    return pd.DataFrame(cross_ref)


def generate_risk_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Generate vendor risk indicators"""
    
    vendor_risk = []
    
    # Group by vendor
    for vendor, group in df.groupby("vendor_name"):
        
        risk_score = 0
        risk_factors = []
        
        # Multiple exclusions
        exclusion_count = len(group[group["record_type"] == "exclusion"])
        if exclusion_count > 0:
            risk_score += exclusion_count * 10
            risk_factors.append(f"{exclusion_count} exclusion(s)")
        
        # High-value contracts (if applicable)
        if "value" in group.columns:
            contract_values = group[group["record_type"] == "contract_award"]["value"]
            if not contract_values.empty and contract_values.notna().any():
                max_value = contract_values.max()
                if max_value > 1000000:  # Threshold
                    risk_score += 5
                    risk_factors.append(f"High-value contract ({max_value:,.0f})")
        
        # Multiple countries
        countries = group["country"].nunique()
        if countries > 1:
            risk_score += 15
            risk_factors.append("Cross-border activity")
        
        if risk_score > 0:
            vendor_risk.append({
                "vendor_name": vendor,
                "risk_score": risk_score,
                "risk_factors": "; ".join(risk_factors),
                "total_records": len(group),
                "countries": ", ".join(group["country"].unique())
            })
    
    risk_df = pd.DataFrame(vendor_risk)
    if not risk_df.empty:
        risk_df = risk_df.sort_values("risk_score", ascending=False)
    
    return risk_df


def generate_transparency_metrics(df: pd.DataFrame) -> dict:
    """Calculate transparency and data quality metrics"""
    
    metrics = {
        "completeness": {
            "vendor_name": (df["vendor_name"].notna().sum() / len(df)) * 100,
            "government_identifier": (df["government_identifier"].notna().sum() / len(df)) * 100,
            "record_date": (df["record_date"].notna().sum() / len(df)) * 100,
            "value": (df["value"].notna().sum() / len(df)) * 100,
            "notes": (df["notes"].notna().sum() / len(df)) * 100
        },
        "data_quality_score": None,
        "coverage": {
            "us_sources": 1,
            "uzbek_sources": len(df[df["country"] == "Uzbekistan"]["source_url"].unique()),
            "total_sources": len(df["source_url"].unique())
        }
    }
    
    # Calculate overall data quality score
    completeness_avg = sum(metrics["completeness"].values()) / len(metrics["completeness"])
    metrics["data_quality_score"] = round(completeness_avg, 2)
    
    return metrics


def generate_comprehensive_analysis():
    """Generate full analysis report"""
    
    print("=" * 80)
    print("FOIA/OPEN DATA ANALYSIS REPORT")
    print("Procurement Risk and Transparency Assessment")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    print()
    
    # Load data
    print("Loading dataset...")
    df = load_dataset()
    print(f"Records loaded: {len(df):,}")
    print()
    
    # Summary Statistics
    print("-" * 80)
    print("DATASET SUMMARY")
    print("-" * 80)
    stats = generate_summary_statistics(df)
    print(f"Total Records: {stats['total_records']:,}")
    print(f"Unique Vendors: {stats['unique_vendors']:,}")
    print(f"Countries Covered: {stats['countries']}")
    print(f"Date Range: {stats['date_range_start']} to {stats['date_range_end']}")
    print(f"US Exclusions: {stats['us_exclusions']:,}")
    print(f"Uzbek Contract Awards: {stats['uzbek_awards']:,}")
    print(f"Records with Value Data: {stats['records_with_value']:,}")
    if stats['total_contract_value'] > 0:
        print(f"Total Contract Value: {stats['total_contract_value']:,.0f} UZS")
    print()
    
    # Exclusions Analysis
    print("-" * 80)
    print("US SAM EXCLUSIONS ANALYSIS")
    print("-" * 80)
    excl_analysis = analyze_exclusions(df)
    if not excl_analysis.empty:
        analysis_dict = excl_analysis.iloc[0].to_dict()
        print(f"Total Exclusions: {analysis_dict['total_exclusions']:,}")
        print(f"Unique Excluded Vendors: {analysis_dict['unique_excluded_vendors']:,}")
        print("\nTop Excluding Agencies:")
        for agency, count in list(analysis_dict['top_agencies'].items())[:5]:
            print(f"  {agency}: {count:,}")
        
        excl_analysis.to_csv(OUTPUT_DIR / "exclusions_analysis.csv", index=False)
    print()
    
    # Uzbek Contracts Analysis
    print("-" * 80)
    print("UZBEKISTAN CONTRACTS ANALYSIS")
    print("-" * 80)
    contract_analysis = analyze_uzbek_contracts(df)
    value_stats = contract_analysis['value_stats']
    print(f"Total Contracts: {value_stats['total_contracts']:,}")
    print(f"Contracts with Value: {value_stats['contracts_with_value']:,}")
    if value_stats['total_value'] > 0:
        print(f"Total Value: {value_stats['total_value']:,.0f} UZS")
        print(f"Mean Value: {value_stats['mean_value']:,.0f} UZS")
        print(f"Median Value: {value_stats['median_value']:,.0f} UZS")
        print(f"Max Value: {value_stats['max_value']:,.0f} UZS")
    
    print("\nTop Vendors by Contract Count:")
    for vendor, count in list(contract_analysis['top_vendors_by_count'].items())[:5]:
        print(f"  {vendor}: {count}")
    
    contract_stats_df = pd.DataFrame([value_stats])
    contract_stats_df.to_csv(OUTPUT_DIR / "uzbek_contracts_stats.csv", index=False)
    print()
    
    # Cross-Reference Analysis
    print("-" * 80)
    print("CROSS-BORDER VENDOR ANALYSIS")
    print("-" * 80)
    cross_ref = cross_reference_analysis(df)
    if not cross_ref.empty:
        print(f"Potential cross-border vendors found: {len(cross_ref)}")
        print("(Requires manual verification with fuzzy matching)")
        cross_ref.to_csv(OUTPUT_DIR / "cross_border_vendors.csv", index=False)
    else:
        print("No exact name matches found between jurisdictions")
    print()
    
    # Risk Analysis
    print("-" * 80)
    print("VENDOR RISK ANALYSIS")
    print("-" * 80)
    risk_df = generate_risk_indicators(df)
    if not risk_df.empty:
        print(f"Vendors with risk indicators: {len(risk_df)}")
        print("\nTop 5 High-Risk Vendors:")
        print(risk_df.head(5).to_string(index=False))
        risk_df.to_csv(OUTPUT_DIR / "vendor_risk_indicators.csv", index=False)
    else:
        print("No high-risk indicators identified")
    print()
    
    # Transparency Metrics
    print("-" * 80)
    print("DATA TRANSPARENCY METRICS")
    print("-" * 80)
    transparency = generate_transparency_metrics(df)
    print("Data Completeness:")
    for field, pct in transparency['completeness'].items():
        print(f"  {field}: {pct:.1f}%")
    print(f"\nOverall Data Quality Score: {transparency['data_quality_score']}%")
    print(f"\nData Sources:")
    print(f"  US Sources: {transparency['coverage']['us_sources']}")
    print(f"  Uzbek Sources: {transparency['coverage']['uzbek_sources']}")
    print(f"  Total Unique Sources: {transparency['coverage']['total_sources']}")
    
    transparency_df = pd.DataFrame([{
        "metric": k, 
        "value": v
    } for k, v in transparency['completeness'].items()])
    transparency_df.to_csv(OUTPUT_DIR / "transparency_metrics.csv", index=False)
    print()
    
    # Save summary
    summary_df = pd.DataFrame([stats])
    summary_df.to_csv(OUTPUT_DIR / "dataset_summary.csv", index=False)
    
    print("=" * 80)
    print(f"ANALYSIS COMPLETE - All outputs saved to: {OUTPUT_DIR}")
    print("=" * 80)


if __name__ == "__main__":
    generate_comprehensive_analysis()
