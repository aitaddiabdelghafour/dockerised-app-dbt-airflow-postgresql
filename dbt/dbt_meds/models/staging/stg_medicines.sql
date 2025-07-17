with source as (

    select * from {{ source('raw', 'medicine_dataset') }}

),

     renamed as (
         SELECT
             trim("Name") AS drug_name,
             trim("Category") AS drug_category,
             lower(trim("Dosage Form")) AS dosage_form,
             CAST(regexp_replace(trim("Strength"), '[^0-9]', '', 'g') AS INTEGER) AS strength_mg,
             trim("Manufacturer") AS manufacturer,
             lower(trim("Indication")) AS indication,
             lower(trim("Classification")) AS classification
         FROM {{ source('raw', 'medicine_dataset') }}
     )

select distinct * from renamed
