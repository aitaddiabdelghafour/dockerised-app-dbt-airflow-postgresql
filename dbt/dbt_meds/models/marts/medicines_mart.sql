with drugs as (
    select * from {{ ref('stg_medicines') }}
),

     category_summary as (
         select
             drug_category,
             count(*) as total_drugs,
             count(distinct manufacturer) as unique_manufacturers,
             avg(strength_mg) as avg_strength_mg,
             count(*) filter (where classification = 'prescription') as prescription_count,
             count(*) filter (where classification = 'over-the-counter') as otc_count
         from drugs
         group by drug_category
     )

select * from category_summary
