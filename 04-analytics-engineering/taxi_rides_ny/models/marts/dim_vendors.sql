-- Dimension table for taxi technology vendors
-- Small static dimension defining vendor codes and their company names

with trips as (
    select * from {{ ref('fct_trips') }}
),

-- 여기서 CASE WHEN 구문으로 명시해버리면, 새로운 vender가 들어오거나 이름이 수정되면 피곤해짐
-- macros 활용!
vendors as (
    select distinct
        vendor_id,
        {{ get_vendor_data('vendor_id') }} as vendor_name
    from trips
)

select * from vendors
