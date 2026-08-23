"""Reusable MWAA-trigger dataset reference and filtering helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


MWAA_DATASETS = [
    "ds_conn_s3_angen_maf_target_ki_au",
    "ds_s3_conn_anz_enrichment_sales_product_map",
    "ds_conn_s3_Inventory_and_Sales_Report",
    "ds_conn_s3_aspen_wholesaler_soh_sample",
    "ds_s3_conn_anz_enrichment_brand_ta_mapping",
    "ds_conn_s3_Stock_On_Hand_and_Sales_by_Product_and_DC",
    "ds_conn_s3_angen_maf_active_msl_cn",
    "ds_conn_s3_angen_maf_planned_msl_cn",
    "ds_conn_s3_angen_maf_target_ki_cn",
    "ds_s3_conn_anz_enrichment_conversion_factors",
    "ds_s3_conn_anz_enrichment_exfactory_nts_pricing",
    "ds_s3_raw_janssen_exfactory_price_sku_indication",
    "ds_s3_conn_anz_enrichment_patient_factors",
    "ds_s3_conn_anz_enrichment_indication_conversion_factors",
    "ds_conn_s3_angen_maf_active_msl_jp",
    "ds_conn_s3_angen_maf_planned_msl_jp",
    "ds_conn_s3_angen_maf_target_ki_jp",
    "ds_conn_s3_angen_maf_kr_ki_list",
    "ds_conn_s3_angen_maf_active_msl_kr",
    "ds_conn_s3_angen_maf_planned_msl_kr",
    "ds_conn_s3_angen_maf_target_ki_kr",
    "ds_s3_conn_anz_enrichment_manual_indication_override",
    "ds_redshift_region_pixonomy_itg_h1_hcp_match_au",
    "ds_redshift_region_pixonomy_itg_h1_hcp_match_jp",
    "ds_redshift_region_pixonomy_itg_h1_hcp_match_kr",
    "ds_redshift_region_pixonomy_itg_h1_hcp_match_nz",
    "ds_redshift_region_pixonomy_stg_h1_activity_counts",
    "ds_redshift_region_pixonomy_stg_h1_address",
    "ds_redshift_region_pixonomy_stg_h1_association",
    "ds_redshift_region_pixonomy_stg_h1_clinical_leader",
    "ds_redshift_region_pixonomy_stg_h1_clinical_trial",
    "ds_redshift_region_pixonomy_stg_h1_company_collaboration",
    "ds_redshift_region_pixonomy_stg_h1_crm_list_jp",
    "ds_redshift_region_pixonomy_stg_h1_digital_reach_focus_area",
    "ds_redshift_region_pixonomy_stg_h1_event_attendee",
    "ds_redshift_region_pixonomy_stg_h1_focus_area",
    "ds_redshift_region_pixonomy_stg_h1_hcp_match_au",
    "ds_redshift_region_pixonomy_stg_h1_hcp_match_jp",
    "ds_redshift_region_pixonomy_stg_h1_hcp_match_kr",
    "ds_redshift_region_pixonomy_stg_h1_hcp_match_nz",
    "ds_redshift_region_pixonomy_stg_h1_index",
    "ds_redshift_region_pixonomy_stg_h1_medical_event",
    "ds_redshift_region_pixonomy_stg_h1_publication",
    "ds_redshift_region_pixonomy_stg_h1_scientific_expert",
    "ds_redshift_region_pixonomy_stg_h1_scientific_expert_changelog",
    "ds_redshift_region_pixonomy_stg_h1_scientific_expert_clinical_trial",
    "ds_redshift_region_pixonomy_stg_h1_scientific_expert_publication",
    "ds_redshift_region_pixonomy_stg_h1_scientificreach_mapping",
    "ds_s3_conn_anz_enrichment_postcode_to_state_mapping",
    "ds_conn_s3_Tier_2_Replenishment_E3_Buyers_Report",
    "ds_s3_conn_anz_enrichment_sku_pbs_indication",
    "ds_s3_conn_anz_enrichment_sku_pbs_indication_delete",
    "ds_conn_s3_Stock_Status_by_DC_13_Months_Sales",
    "ds_s3_conn_anz_enrichment_pbs_indication_delete",
    "ds_redshift_region_pixonomy_dm_angen_market_combined_verification_angen_unload",
]


@dataclass(frozen=True)
class MWAADatasetReference:
    datasets: frozenset[str]

    @classmethod
    def default(cls) -> "MWAADatasetReference":
        return cls(frozenset(MWAA_DATASETS))

    def contains(self, dataset: str | None) -> bool:
        return str(dataset) in self.datasets if dataset is not None else False

    def to_dataframe(self, column_name: str = "dataset") -> pd.DataFrame:
        return pd.DataFrame({column_name: sorted(self.datasets)})

    def filter_dataframe(
        self,
        df: pd.DataFrame,
        dataset_col: str = "dataset",
        include: bool = False,
    ) -> pd.DataFrame:
        if df.empty or dataset_col not in df.columns:
            return df.copy()
        mask = df[dataset_col].astype(str).isin(self.datasets)
        return df[mask].copy() if include else df[~mask].copy()
