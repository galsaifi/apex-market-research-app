import streamlit as st
import pandas as pd
import pandasql as ps

# Set up the Streamlit page
st.set_page_config(page_title="USASpending Data Analysis Dashboard", layout="wide")

st.title("APEX Market Research Dashboard")

# Placeholder for datasets
datasets = {}

# File upload sections with side-by-side layout
st.write("### Upload Datasets")
col1, col2 = st.columns(2)

with col1:
    st.write("#### NAICS Code Datasets")
    uploaded_file_1 = st.file_uploader("Upload Prime-Contract Dataset Using NAICS Codes", type=["csv", "xlsx"], key="uploader_1")

    if uploaded_file_1:
        try:
            datasets["Dataset 1"] = pd.read_csv(uploaded_file_1) if uploaded_file_1.name.endswith(".csv") else pd.read_excel(uploaded_file_1)
            st.success("Prime-Contract NAICS Dataset successfully uploaded.")
        except Exception as e:
            st.error(f"Error loading Prime-Contract NAICS Dataset: {e}")

with col2:
    st.write("#### PSC Code Datasets")
    uploaded_file_3 = st.file_uploader("Upload Prime-Contract Dataset Using PSC Codes (Optional)", type=["csv", "xlsx"], key="uploader_3")

    if uploaded_file_3:
        try:
            datasets["Dataset 3"] = pd.read_csv(uploaded_file_3) if uploaded_file_3.name.endswith(".csv") else pd.read_excel(uploaded_file_3)
            st.success("Prime-Contract PSC Dataset successfully uploaded.")
        except Exception as e:
            st.error(f"Error loading Prime-Contract PSC Dataset: {e}")

# Extract NAICS codes from the first dataset and PSC codes from the third dataset
naics_codes = []
psc_codes = []

if "Dataset 1" in datasets and "naics_code" in datasets["Dataset 1"].columns:
    naics_codes = datasets["Dataset 1"]["naics_code"].dropna().astype(str).unique().tolist()

if "Dataset 3" in datasets and "product_or_service_code" in datasets["Dataset 3"].columns:
    psc_codes = sorted(datasets["Dataset 3"]["product_or_service_code"].dropna().unique())

# Display extracted NAICS and PSC codes
st.write("### Extracted NAICS and PSC Codes")
st.write("#### NAICS Codes:")
if naics_codes:
    st.markdown("\n".join([f"- {code}" for code in naics_codes]))
else:
    st.write("No NAICS Codes found in the dataset.")

st.write("#### PSC Codes:")
if psc_codes:
    st.markdown("\n".join([f"- {code}" for code in psc_codes]))
else:
    st.write("No PSC Codes found in the dataset.")

# Keyword input
st.write("### Enter Keywords (Optional)")
keywords_input = st.text_area("Enter Keywords (comma-separated):")
keywords = [keyword.strip() for keyword in keywords_input.split(",") if keyword.strip()]

# Display entered keywords
st.write("#### Entered Keywords:")
if keywords:
    st.markdown("\n".join([f"- {keyword}" for keyword in keywords]))
else:
    st.write("No Keywords provided.")

# Button to concatenate datasets and run analysis
if st.button("Start Analysis"):
    try:
        # NAICS datasets
        naics_primes = datasets.get("Dataset 1")
        naics_subs = datasets.get("Dataset 2")

        # Prepare lists of datasets for concatenation
        primes_to_combine = [df for df in [naics_primes, datasets.get("Dataset 3")] if df is not None]

        # Combine primes and subs datasets only if there are datasets to combine
        primes_combined = pd.concat(primes_to_combine, ignore_index=True) if primes_to_combine else pd.DataFrame()

        # Filter by keywords if provided
        if not primes_combined.empty and keywords:
            pattern = '|'.join(keywords)  # Create case-insensitive search pattern
            primes_combined = primes_combined[primes_combined['transaction_description'].str.contains(
                pattern, case=False, na=False)]
        
        # Display filtered dataset (optional)
        if not primes_combined.empty:
            st.write("### Filtered Dataset")
            st.write(primes_combined.head())
            st.write(f"Shape: {primes_combined.shape}")

        if primes_combined.empty:
            st.warning("No Prime datasets available after filtering. Please check your keywords.")
        else:
            st.write("### Analysis")
            
            # Top Agency Query
            query="""
            SELECT
                awarding_agency_name,
                SUM(total_dollars_obligated) AS total_obligation,
                COUNT(*) AS number_of_transactions
            FROM primes_combined
            WHERE awarding_agency_name IS NOT NULL
            GROUP BY awarding_agency_name
            ORDER BY total_obligation DESC, number_of_transactions DESC
            """
            try:
                top_agencies = ps.sqldf(query, locals())
                st.write("#### Top agencies by spending")
                st.write(top_agencies)
            except Exception as e:
                st.error(f"Error during top agency spending: {e}")

            # NAICS Query
            to_3_agencies = ', '.join(f"'{agency}'" for agency in top_agencies["awarding_agency_name"].head(3))
            list_naics_codes = ', '.join(f"'{code}'" for code in naics_codes)
            query_naics = f"""
            SELECT
                awarding_agency_name,
                CAST(naics_code AS TEXT) AS naics_code,
                SUM(total_dollars_obligated) AS total_obligation,
                COUNT(*) AS number_of_transactions
            FROM primes_combined
            WHERE 
                naics_code IN ({list_naics_codes})
                AND awarding_agency_name IN ({to_3_agencies})
            GROUP BY awarding_agency_name, naics_code
            ORDER BY awarding_agency_name, naics_code, total_obligation DESC
            """
            try:
                naics_spending = ps.sqldf(query_naics, locals())
                st.write("#### Agency Spending on Selected NAICS Codes")
                st.write(naics_spending)
            except Exception as e:
                st.error(f"Error during NAICS analysis: {e}")


            # PSC Query with corrected column name
            list_psc_codes = ', '.join(f"'{code}'" for code in psc_codes)
            query_psc = f"""
            SELECT
                awarding_agency_name,
                product_or_service_code AS psc_code,
                SUM(total_dollars_obligated) AS total_obligation,
                COUNT(*) AS number_of_transactions
            FROM primes_combined
            WHERE 
                product_or_service_code IN ({list_psc_codes})
                AND awarding_agency_name IN ({to_3_agencies})
            GROUP BY awarding_agency_name, product_or_service_code
            ORDER BY awarding_agency_name, product_or_service_code, total_obligation DESC
            """
            try:
                psc_spending = ps.sqldf(query_psc, locals())
                st.write("#### Agency Spending on Selected PSC Codes")
                st.write(psc_spending)
            except Exception as e:
                st.error(f"Error during PSC analysis: {e}")


            query_sb_percentages = f"""
            WITH sb_data AS (
                SELECT
                    awarding_agency_name,
                    contracting_officers_determination_of_business_size AS set_aside,
                    SUM(total_dollars_obligated) AS total_obligation
                FROM primes_combined
                WHERE awarding_agency_name IN ({to_3_agencies})
                GROUP BY awarding_agency_name, set_aside
            ),
            agency_totals AS (
                SELECT 
                    awarding_agency_name,
                    SUM(total_dollars_obligated) AS total_obligation
                FROM primes_combined
                WHERE awarding_agency_name IN ({to_3_agencies})
                GROUP BY awarding_agency_name
            )
            SELECT 
                sb.awarding_agency_name,
                sb.set_aside,
                sb.total_obligation,
                ROUND((sb.total_obligation * 100.0 / at.total_obligation), 2) AS percentage_of_total
            FROM sb_data AS sb
            JOIN agency_totals AS at
            ON sb.awarding_agency_name = at.awarding_agency_name
            ORDER BY sb.awarding_agency_name, sb.set_aside DESC, sb.total_obligation DESC
            """

            try:
                st.write("#### Top Agencies' Spending on Small Business")
                top_agencies_sb_percentage = ps.sqldf(query_sb_percentages, locals())
                st.write(top_agencies_sb_percentage)
            except Exception as e:
                st.error(f"Error during agency small business spending analysis: {e}")


            query_set_aside = f"""
            WITH sb_data AS (
                SELECT
                    awarding_agency_name,
                    contracting_officers_determination_of_business_size AS set_aside,
                    SUM(total_dollars_obligated) AS total_obligation
                FROM primes_combined
                WHERE awarding_agency_name IN ({to_3_agencies})
                GROUP BY awarding_agency_name, set_aside
            ),
            agency_totals AS (
                SELECT 
                    awarding_agency_name,
                    SUM(total_dollars_obligated) AS total_obligation
                FROM primes_combined
                WHERE awarding_agency_name IN ({to_3_agencies})
                GROUP BY awarding_agency_name
            )
            SELECT 
                sb.awarding_agency_name,
                sb.set_aside,
                sb.total_obligation,
                ROUND((sb.total_obligation * 100.0 / at.total_obligation), 2) AS percentage_of_total
            FROM sb_data AS sb
            JOIN agency_totals AS at
            ON sb.awarding_agency_name = at.awarding_agency_name
            ORDER BY sb.awarding_agency_name, sb.set_aside DESC, sb.total_obligation DESC
            """

            try:
                st.write("#### Top Agency Set-Aside Spending")
                top_agencies_set_aside = ps.sqldf(query_set_aside, locals())
                st.write(top_agencies_set_aside)
            except Exception as e:
                st.error(f"Error during agency agency set-aside spending: {e}")


            query_top_primes="""
            SELECT
                recipient_name,
                recipient_uei,
                SUM(total_dollars_obligated) AS total_obligation,
                count(*) AS number_of_transactions,
                contracting_officers_determination_of_business_size,
                organizational_type
            FROM primes_combined
            GROUP BY recipient_uei
            ORDER BY total_obligation DESC, number_of_transactions DESC
            """


            try:
                st.write("#### Top Primes in Defined NAICS/PSC Codes")
                top_recipients = ps.sqldf(query_top_primes, locals())
                st.write(top_recipients)
            except Exception as e:
                st.error(f"Error during top primes analysis: {e}")


            query_sap=f"""
            SELECT
                awarding_agency_name,
                simplified_procedures_for_certain_commercial_items,
                SUM(total_dollars_obligated) AS total_obligation,
                COUNT(*) AS number_of_transactions,
                ROUND(SUM(total_dollars_obligated) * 100.0 / SUM(SUM(total_dollars_obligated)) OVER (PARTITION BY awarding_agency_name), 2) AS percentage_of_obligation
            FROM primes_combined
            WHERE 
                awarding_agency_name IN ({to_3_agencies})
                AND simplified_procedures_for_certain_commercial_items != 'None'
            GROUP BY awarding_agency_name, simplified_procedures_for_certain_commercial_items
            ORDER BY awarding_agency_name, simplified_procedures_for_certain_commercial_items DESC, total_obligation DESC
            """

            try:
                st.write("#### Top Agencies SAP Utility")
                top_agencies_sap = ps.sqldf(query_sap, locals())
                st.write(top_agencies_sap)
            except Exception as e:
                st.error(f"Error during top agency SAP utility: {e}")


            query_award_type=f"""
            SELECT
                awarding_agency_name,
                award_type,
                COUNT(*) AS number_of_transactions,
                SUM(total_dollars_obligated) AS total_obligation
            FROM primes_combined
            WHERE awarding_agency_name IN ({to_3_agencies})
            GROUP BY awarding_agency_name, award_type
            ORDER BY awarding_agency_name, award_type, total_obligation DESC, number_of_transactions DESC
            """

            try:
                st.write("#### Top Agencies Preferred Buying Methods")
                top_agencies_award_type = ps.sqldf(query_award_type, locals())
                st.write(top_agencies_award_type)
            except Exception as e:
                st.error(f"Error during top agency preferred buying methods: {e}")

    except Exception as e:
        st.error(f"Error during combination: {e}")