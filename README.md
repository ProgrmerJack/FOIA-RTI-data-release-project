# FOIA / RTI “Already-Released” Republisher

This workspace assembles procurement-risk records already released by U.S. and Uzbek authorities, normalises them, and packages the result for Zenodo distribution (dataset + codebook).

## Source holdings

- `Data/USA/samexclusionspublicextract-gsa-1626.csv` — GSA SAM Exclusions Public Extract (mirror downloaded 2 Nov 2025).  
- `Data/Uzbekistan/*.csv` — Ministry of Finance procurement disclosures from https://data.egov.uz (dataset IDs embedded in filenames).  
- Optional ZIP mirrors (`2024-FOIASetFull.zip`, etc.) retained for provenance.

## Build instructions
```powershell
py -3 -X utf8 build_dataset.py
```
Outputs:
- `outputs/foia_vendor_risk_dataset.csv` — unified table (~150 k rows).  
- `outputs/foia_vendor_risk_codebook.csv` — column definitions for deposit.

## Schema highlights
- `country` — publishing jurisdiction (`United States`, `Uzbekistan`).  
- `record_type` — `exclusion` (SAM) or `contract_award` (Uzbekistan portal).  
- `vendor_name`, `government_identifier`, `record_id`, `record_date`.  
- `value`, `currency` — monetary fields when provided (Uzbek contracts).  
- `notes` — concatenated metadata (agency + exclusion program, or purchase type + platform).  
- `source_url` — landing page for the originating dataset (GSA Open Data / data.egov.uz dataset ID).

## Zenodo deposition checklist
Use `zenodo_deposit_checklist.md` to populate the deposit form. Attach:
1. `outputs/foia_vendor_risk_dataset.csv` (optionally gzip).  
2. `outputs/foia_vendor_risk_codebook.csv`.  
3. This `README.md` documenting processing + citations.  
4. Provenance note citing:  
   - SAM Exclusions API registry — https://open.gsa.gov/api/sam/ (public domain, 17 U.S.C. § 105).  
   - Uzbekistan open data portal dataset IDs `1-012-0011`, `3-001-0006`, `3-011-0001`, `3-033-0006`, `4-069-0005`, `5-001-0015` (Open Data — free use licence).

## Next steps
- Run `outputs/foia_vendor_risk_dataset.csv` through spell-check + duplicate detection before final DOI publish.  
- Consider lightweight data dictionary PDF for Zenodo “related/other” files.  
- After DOI minting, update `zenodo_deposit_checklist.md` with the assigned DOI and embargo status (if any).
