import streamlit as st
import pandas as pd
import pandasql as ps
import io


# Set up the Streamlit page
st.set_page_config(page_title="USASpending Data Analysis Dashboard", layout="wide")

hide_streamlit_style = """
            <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            </style>
            """
st.markdown(hide_streamlit_style, unsafe_allow_html=True) 

st.title("APEX Market Research App")

# Placeholder for datasets
datasets = {}

# File upload sections with side-by-side layout
st.write("### Upload Datasets")
col1, col2 = st.columns(2)

with col1:
    st.write("#### NAICS Code Dataset")
    uploaded_file_1 = st.file_uploader("Upload Prime-Contract Dataset Using NAICS Codes", type=["csv", "xlsx"], key="uploader_1")

    if uploaded_file_1:
        try:
            datasets["Dataset 1"] = pd.read_csv(uploaded_file_1) if uploaded_file_1.name.endswith(".csv") else pd.read_excel(uploaded_file_1)
            st.success("Prime-Contract NAICS Dataset successfully uploaded.")
        except Exception as e:
            st.error(f"Error loading Prime-Contract NAICS Dataset: {e}")

with col2:
    st.write("#### PSC Code Dataset")
    uploaded_file_3 = st.file_uploader("Upload Prime-Contract Dataset Using PSC Codes (Optional)", type=["csv", "xlsx"], key="uploader_3")

    if uploaded_file_3:
        try:
            datasets["Dataset 3"] = pd.read_csv(uploaded_file_3) if uploaded_file_3.name.endswith(".csv") else pd.read_excel(uploaded_file_3)
            st.success("Prime-Contract PSC Dataset successfully uploaded.")
        except Exception as e:
            st.error(f"Error loading Prime-Contract PSC Dataset: {e}")

# Display options for NAICS, PSC, and keywords only if Dataset 1 is loaded
if "Dataset 1" in datasets:
    # Extract NAICS codes from the first dataset and PSC codes from the third dataset
    naics_codes = datasets["Dataset 1"]["naics_code"].dropna().astype(str).unique().tolist() if "naics_code" in datasets["Dataset 1"].columns else []
    psc_codes = sorted(datasets["Dataset 3"]["product_or_service_code"].dropna().unique()) if "Dataset 3" in datasets and "product_or_service_code" in datasets["Dataset 3"].columns else []

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
    if st.button("Generate Results"):
        try:
            # Combine datasets
            naics_primes = datasets.get("Dataset 1")
            psc_primes = datasets.get("Dataset 3")
            primes_combined = pd.concat([naics_primes, psc_primes], ignore_index=True) if naics_primes is not None else pd.DataFrame()

            if not primes_combined.empty and "transaction_description" in primes_combined.columns and keywords:
                pattern = '|'.join(keywords)
                primes_combined = primes_combined[primes_combined['transaction_description'].str.contains(pattern, case=False, na=False)]

            if primes_combined.empty:
                st.warning("No Prime datasets available after filtering. Please check your data.")
            else:
                st.write("### Analysis")

                # Top Agencies by Spending
                query_top_agencies = """
                SELECT
                    awarding_agency_name,
                    SUM(total_dollars_obligated) AS total_obligation,
                    COUNT(*) AS number_of_transactions
                FROM primes_combined
                GROUP BY awarding_agency_name
                ORDER BY total_obligation DESC, number_of_transactions DESC
                """
                
                st.session_state["results"] = {}
                try:
                    top_agencies = ps.sqldf(query_top_agencies, locals())
                    top_agencies.columns = ["Awarding Agency", "Total Spending", "Number of Transactions"]
                    st.session_state["results"]["Agency Spendings"] = top_agencies
                except Exception as e:
                    st.error(f"Error during top agency analysis: {e}")


                # NAICS Query
                to_3_agencies = ', '.join(f"'{agency}'" for agency in top_agencies["Awarding Agency"].head(3))
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
                    naics_spending.columns = ["Awarding Agency", "NAICS Code", "Total Spending", "Number of Transactions"]
                    st.session_state["results"]["Agency Spending on NAICS Codes"] = naics_spending
                except Exception as e:
                    st.error(f"Error during NAICS analysis: {e}")


                # PSC Query
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

                # Run query and save result
                try:
                    psc_spending = ps.sqldf(query_psc, locals())
                    psc_spending.columns = ["Awarding Agency", "PSC Code", "Total Spending", "Number of Transactions"]
                    st.session_state["results"]["Agency Spending on PSC Codes"] = psc_spending
                except Exception as e:
                    st.error(f"Error during PSC analysis: {e}")


                # Top agency spending by business size.
                query_sb_percentages = f"""
                WITH sb_data AS (
                    SELECT
                        awarding_agency_name,
                        contracting_officers_determination_of_business_size AS business_size,
                        SUM(total_dollars_obligated) AS total_obligation
                    FROM primes_combined
                    WHERE awarding_agency_name IN ({to_3_agencies})
                    GROUP BY awarding_agency_name, business_size
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
                    sb.business_size,
                    sb.total_obligation,
                    ROUND((sb.total_obligation * 100.0 / at.total_obligation), 2) AS percentage_of_total
                FROM sb_data AS sb
                JOIN agency_totals AS at
                ON sb.awarding_agency_name = at.awarding_agency_name
                ORDER BY sb.awarding_agency_name, sb.business_size DESC, sb.total_obligation DESC
                """

                try:
                    small_business_spending = ps.sqldf(query_sb_percentages, locals())
                    small_business_spending.columns = ["Awarding Agency", "Business Size", "Total Spending", 
                                                       "Percentage of Total Spending"]
                    st.session_state["results"]["Agency Small Business Spending"] = small_business_spending
                except Exception as e:
                    st.error(f"Error during agency small business spending analysis: {e}")



                # Top agency spending on different set-asides
                primes_combined["type_of_set_aside"] = primes_combined["type_of_set_aside"].fillna("NO SET ASIDE USED.")

                query_set_aside = f"""
                WITH sb_data AS (
                    SELECT
                        awarding_agency_name,
                        type_of_set_aside AS set_aside,
                        SUM(total_dollars_obligated) AS total_obligation
                    FROM primes_combined
                    WHERE awarding_agency_name IN ({to_3_agencies})
                    GROUP BY awarding_agency_name, type_of_set_aside
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
                    sb.awarding_agency_name AS agency,
                    sb.set_aside,
                    sb.total_obligation AS set_aside_spending,
                    ROUND((sb.total_obligation * 100.0 / at.total_obligation), 2) AS percentage_of_total_spending
                FROM sb_data AS sb
                JOIN agency_totals AS at
                ON sb.awarding_agency_name = at.awarding_agency_name
                ORDER BY sb.awarding_agency_name, sb.total_obligation DESC, sb.set_aside DESC
                """

                try:
                    set_aside_spending = ps.sqldf(query_set_aside, locals())
                    set_aside_spending.columns = ["Awarding Agency", "Set-Aside", "Total Spending", "Percentage of Total Spending"]
                    st.session_state["results"]["Agency Set-Aside Spending"] = set_aside_spending
                except Exception as e:
                    st.error(f"Error during agency set-aside spending analysis: {e}")


                # Top primes in the defined NAICS/PSC codes
                query_top_primes = """
                SELECT
                    recipient_name,
                    recipient_uei,
                    SUM(total_dollars_obligated) AS total_obligation,
                    COUNT(*) AS number_of_transactions,
                    contracting_officers_determination_of_business_size,
                    organizational_type
                FROM primes_combined
                GROUP BY recipient_uei
                ORDER BY total_obligation DESC, number_of_transactions DESC
                """

                try:
                    top_primes = ps.sqldf(query_top_primes, locals())
                    top_primes.columns = ["Recipient Name", "Recipient UEI", "Total Spending", "Number of Transactions",
                                        "Business Size", "Organizational Type"]
                    st.session_state["results"]["Top Primes"] = top_primes
                except Exception as e:
                    st.error(f"Error during top primes analysis: {e}")


                # Top agencies' SAP utility
                query_sap = f"""
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

                # Run query and save result
                try:
                    top_agencies_sap = ps.sqldf(query_sap, locals())
                    top_agencies_sap.columns = ["Awarding Agency", "SAP Utility", "Total Spending", "Number of Transactions", 
                                                "Percentage of Obligation"]
                    st.session_state["results"]["Agency SAP Utility"] = top_agencies_sap
                except Exception as e:
                    st.error(f"Error during top agency SAP utility: {e}")


                # Top agencies' preferred buying methods
                query_award_type = f"""
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
                    top_agencies_award_type = ps.sqldf(query_award_type, locals())
                    top_agencies_award_type.columns = ["Awarding Agency", "Award Type", "Number of Transactions", "Total Obligation"]
                    st.session_state["results"]["Agency Preferred Buying Methods"] = top_agencies_award_type
                except Exception as e:
                    st.error(f"Error during top agency preferred buying methods: {e}")


        except Exception as e:
            st.error(f"Error data processing: {e}")


if "results" in st.session_state and st.session_state["results"]:
    results_list = list(st.session_state["results"].items())
    for idx, (result_name, result_df) in enumerate(results_list):
        st.write(f"### {result_name}")
        st.write(result_df)

        if not result_df.empty:
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
                result_df.to_excel(writer, index=False, sheet_name=result_name[:31])
            buffer.seek(0)

            st.download_button(
                label="Download",
                data=buffer,
                file_name=f"{result_name.replace(' ', '_').lower()}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

        # Add a divider line if not the last iteration
        if idx < len(results_list) - 1:
            st.write("---")