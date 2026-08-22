/* ============================================================
   4. GOLD REFRESH PROCEDURE
   ============================================================ */

CREATE   PROCEDURE ops.usp_refresh_gold_core
AS
BEGIN

    SET NOCOUNT ON;

    DECLARE @gold_run_id VARCHAR(100) = CONVERT(VARCHAR(100), NEWID());
    DECLARE @started_at_utc DATETIME2(6) = SYSUTCDATETIME();

    /* --------------------------------------------------------
       Procurement category dimension
       -------------------------------------------------------- */

    MERGE dim.procurement_category AS t
    USING
    (
        SELECT *
        FROM (VALUES
            (1, 'Digital Services',          1),
            (2, 'Construction & Works',      2),
            (3, 'Professional Services',     3),
            (4, 'Facilities & Maintenance',  4),
            (5, 'Goods & Supplies',          5),
            (6, 'Healthcare Supplies',       6),
            (7, 'Transport & Logistics',     7),
            (8, 'Energy & Utilities',        8)
        ) v(category_id, category, sort_order)
    ) AS s
      ON t.category_id = s.category_id

    WHEN MATCHED THEN UPDATE SET
        t.category = s.category,
        t.sort_order = s.sort_order,
        t.is_active = 1,
        t.gold_refreshed_at_utc = SYSUTCDATETIME()

    WHEN NOT MATCHED THEN INSERT
    (
        category_id,
        category,
        sort_order,
        is_active,
        gold_refreshed_at_utc
    )
    VALUES
    (
        s.category_id,
        s.category,
        s.sort_order,
        1,
        SYSUTCDATETIME()
    );


    /* --------------------------------------------------------
       Source dimension
       -------------------------------------------------------- */

    MERGE dim.source AS t
    USING
    (
        SELECT
            source_system,
            source_display_name,
            source_type,
            enabled_flag,
            supported_entity_types,
            schema_versions,
            is_synthetic,
            silver_run_id,
            processed_at_utc
        FROM [LH_CivicSignal_Silver].[dbo].[silver_sources]
    ) AS s
      ON t.source_system = s.source_system

    WHEN MATCHED THEN UPDATE SET
        t.source_display_name = s.source_display_name,
        t.source_type = s.source_type,
        t.enabled_flag = s.enabled_flag,
        t.supported_entity_types = s.supported_entity_types,
        t.schema_versions = s.schema_versions,
        t.is_synthetic = s.is_synthetic,
        t.silver_run_id = s.silver_run_id,
        t.silver_processed_at_utc = s.processed_at_utc,
        t.gold_refreshed_at_utc = SYSUTCDATETIME()

    WHEN NOT MATCHED THEN INSERT
    (
        source_system,
        source_display_name,
        source_type,
        enabled_flag,
        supported_entity_types,
        schema_versions,
        is_synthetic,
        silver_run_id,
        silver_processed_at_utc,
        gold_refreshed_at_utc
    )
    VALUES
    (
        s.source_system,
        s.source_display_name,
        s.source_type,
        s.enabled_flag,
        s.supported_entity_types,
        s.schema_versions,
        s.is_synthetic,
        s.silver_run_id,
        s.processed_at_utc,
        SYSUTCDATETIME()
    );


    /* --------------------------------------------------------
       Buyer dimension
       -------------------------------------------------------- */

    MERGE dim.buyer AS t
    USING
    (
        SELECT
            buyer_key,
            buyer_id,
            buyer_name,
            buyer_type,
            country,
            country_code,
            region,
            website,
            source_system,
            first_seen_at_utc,
            last_seen_at_utc,
            silver_run_id,
            processed_at_utc
        FROM [LH_CivicSignal_Silver].[dbo].[silver_buyers]
    ) AS s
      ON t.buyer_key = s.buyer_key

    WHEN MATCHED THEN UPDATE SET
        t.buyer_id = s.buyer_id,
        t.buyer_name = s.buyer_name,
        t.buyer_type = s.buyer_type,
        t.country = s.country,
        t.country_code = s.country_code,
        t.region = s.region,
        t.website = s.website,
        t.source_system = s.source_system,
        t.first_seen_at_utc = s.first_seen_at_utc,
        t.last_seen_at_utc = s.last_seen_at_utc,
        t.silver_run_id = s.silver_run_id,
        t.silver_processed_at_utc = s.processed_at_utc,
        t.gold_refreshed_at_utc = SYSUTCDATETIME()

    WHEN NOT MATCHED THEN INSERT
    (
        buyer_key,
        buyer_id,
        buyer_name,
        buyer_type,
        country,
        country_code,
        region,
        website,
        source_system,
        first_seen_at_utc,
        last_seen_at_utc,
        silver_run_id,
        silver_processed_at_utc,
        gold_refreshed_at_utc
    )
    VALUES
    (
        s.buyer_key,
        s.buyer_id,
        s.buyer_name,
        s.buyer_type,
        s.country,
        s.country_code,
        s.region,
        s.website,
        s.source_system,
        s.first_seen_at_utc,
        s.last_seen_at_utc,
        s.silver_run_id,
        s.processed_at_utc,
        SYSUTCDATETIME()
    );


    /* --------------------------------------------------------
       Opportunity fact
       -------------------------------------------------------- */

    MERGE fact.opportunity AS t
    USING
    (
        SELECT
            o.*,
            c.category_id
        FROM [LH_CivicSignal_Silver].[dbo].[silver_opportunities] o
        LEFT JOIN dim.procurement_category c
          ON c.category = o.category_normalized
    ) AS s
      ON t.opportunity_key = s.opportunity_key

    WHEN MATCHED THEN UPDATE SET
        t.opportunity_id = s.opportunity_id,
        t.external_reference = s.external_reference,
        t.title = s.title,
        t.description = s.description,
        t.buyer_key = s.buyer_key,
        t.source_system = s.source_system,
        t.category_id = s.category_id,
        t.buyer_id = s.buyer_id,
        t.buyer_name = s.buyer_name,
        t.category_raw = s.category_raw,
        t.category = s.category_normalized,
        t.category_quality_status = s.category_quality_status,
        t.status_raw = s.status_raw,
        t.status = s.status,
        t.country = s.country,
        t.country_code = s.country_code,
        t.region = s.region,
        t.published_date = s.published_date,
        t.closing_date = s.closing_date,
        t.days_to_close = s.days_to_close,
        t.currency_code = s.currency_code,
        t.estimated_value = s.estimated_value,
        t.official_source_url = s.official_source_url,
        t.duplicate_group_hint = s.duplicate_group_hint,
        t.potential_duplicate_flag = s.potential_duplicate_flag,
        t.source_record_id = s.source_record_id,
        t.first_seen_at_utc = s.first_seen_at_utc,
        t.last_seen_at_utc = s.last_seen_at_utc,
        t.silver_run_id = s.silver_run_id,
        t.silver_processed_at_utc = s.processed_at_utc,
        t.gold_refreshed_at_utc = SYSUTCDATETIME()

    WHEN NOT MATCHED THEN INSERT
    (
        opportunity_key,
        opportunity_id,
        external_reference,
        title,
        description,
        buyer_key,
        source_system,
        category_id,
        buyer_id,
        buyer_name,
        category_raw,
        category,
        category_quality_status,
        status_raw,
        status,
        country,
        country_code,
        region,
        published_date,
        closing_date,
        days_to_close,
        currency_code,
        estimated_value,
        official_source_url,
        duplicate_group_hint,
        potential_duplicate_flag,
        source_record_id,
        first_seen_at_utc,
        last_seen_at_utc,
        silver_run_id,
        silver_processed_at_utc,
        gold_refreshed_at_utc
    )
    VALUES
    (
        s.opportunity_key,
        s.opportunity_id,
        s.external_reference,
        s.title,
        s.description,
        s.buyer_key,
        s.source_system,
        s.category_id,
        s.buyer_id,
        s.buyer_name,
        s.category_raw,
        s.category_normalized,
        s.category_quality_status,
        s.status_raw,
        s.status,
        s.country,
        s.country_code,
        s.region,
        s.published_date,
        s.closing_date,
        s.days_to_close,
        s.currency_code,
        s.estimated_value,
        s.official_source_url,
        s.duplicate_group_hint,
        s.potential_duplicate_flag,
        s.source_record_id,
        s.first_seen_at_utc,
        s.last_seen_at_utc,
        s.silver_run_id,
        s.processed_at_utc,
        SYSUTCDATETIME()
    );


    /* --------------------------------------------------------
       Current duplicate candidates

       Current-state dataset, so full replacement is intentional.
       -------------------------------------------------------- */

    DELETE FROM fact.opportunity_duplicate_candidate;

    INSERT INTO fact.opportunity_duplicate_candidate
    (
        duplicate_group_hint,
        opportunity_key,
        source_system,
        opportunity_id,
        title,
        buyer_name,
        published_date,
        potential_duplicate_flag,
        silver_run_id,
        silver_processed_at_utc,
        gold_refreshed_at_utc
    )
    SELECT
        duplicate_group_hint,
        opportunity_key,
        source_system,
        opportunity_id,
        title,
        buyer_name,
        published_date,
        potential_duplicate_flag,
        silver_run_id,
        processed_at_utc,
        SYSUTCDATETIME()
    FROM [LH_CivicSignal_Silver].[dbo].[silver_opportunity_duplicate_candidates];


    /* --------------------------------------------------------
       Validation / audit
       -------------------------------------------------------- */

    DECLARE @orphan_buyer_rows BIGINT =
    (
        SELECT COUNT_BIG(*)
        FROM fact.opportunity o
        LEFT JOIN dim.buyer b
          ON b.buyer_key = o.buyer_key
        WHERE b.buyer_key IS NULL
    );

    INSERT INTO ops.gold_refresh_runs
    (
        gold_run_id,
        started_at_utc,
        completed_at_utc,
        source_rows,
        buyer_rows,
        category_rows,
        opportunity_rows,
        duplicate_candidate_rows,
        orphan_buyer_rows,
        status
    )
    SELECT
        @gold_run_id,
        @started_at_utc,
        SYSUTCDATETIME(),
        (SELECT COUNT_BIG(*) FROM dim.source),
        (SELECT COUNT_BIG(*) FROM dim.buyer),
        (SELECT COUNT_BIG(*) FROM dim.procurement_category),
        (SELECT COUNT_BIG(*) FROM fact.opportunity),
        (SELECT COUNT_BIG(*) FROM fact.opportunity_duplicate_candidate),
        @orphan_buyer_rows,
        CASE WHEN @orphan_buyer_rows = 0
             THEN 'Succeeded'
             ELSE 'FailedValidation'
        END;

END;