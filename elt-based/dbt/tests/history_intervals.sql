select * from {{ ref('silver_customer_history') }} where valid_to<=valid_from
