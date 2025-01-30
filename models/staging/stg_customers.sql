with

raw_customers as (

    select * from {{ source('ecom', 'raw_customers') }}

),
cleaned AS (
    SELECT 
        name,
        SPLIT(name, ' ') AS name_parts
    FROM raw_customers
),
extracted AS (
    SELECT
        c.name,
        -- Only keep alphabetic letters, remove digits & punctuation
        REGEXP_REPLACE(f.value, '[^a-zA-Z]', '') AS token_raw
    FROM cleaned c
         -- Flatten array with the standard Snowflake syntax:
         , TABLE(FLATTEN(INPUT => c.name_parts)) f
),
filtered AS (
    SELECT
        name,
        token_raw,
        LENGTH(token_raw) AS token_length
    FROM extracted
    /* Exclude known short prefixes/suffixes, plus empty tokens */
    WHERE LOWER(token_raw) NOT IN (
        'mr','mrs','ms',
        'dr','prof','jr','sr',
        'phd','md','dvm','ii','iii'
    )
      AND token_raw <> ''
),
ranked AS (
    SELECT
        name,
        token_raw,
        ROW_NUMBER() OVER (
            PARTITION BY name 
            ORDER BY token_length DESC
        ) AS rn
    FROM filtered
)
SELECT
    name,
    MAX(CASE WHEN rn = 1 THEN token_raw END) AS first_name,
    MAX(CASE WHEN rn = 2 THEN token_raw END) AS last_name
FROM ranked
GROUP BY name