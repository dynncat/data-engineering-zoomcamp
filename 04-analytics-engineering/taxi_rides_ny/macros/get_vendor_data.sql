{#
    Macro to generate vendor_name column using Jinja dictionary.

    This approach works seamlessly across BigQuery, DuckDB, Snowflake, etc.
    by generating a CASE statement at compile time.

    Usage: {{ get_vendor_data('vendor_id') }}
    Returns: SQL CASE expression that maps vendor_id to vendor_name
#}

-- 재사용이 가능한 함수처럼 만들기
-- jinja를 통해서 매핑만 해줘놓으면, 래퍼가 이걸 CASE WHEN 구문으로 바꿔줌
-- 재사용이 가능한, 노가다 안해도 되는 좋은 방식
{% macro get_vendor_data(vendor_id_column) %}

{% set vendors = {
    1: 'Creative Mobile Technologies',
    2: 'VeriFone Inc.',
    4: 'Unknown/Other'
} %}

case {{ vendor_id_column }}
    {% for vendor_id, vendor_name in vendors.items() %}     -- 사전에 있는 항목 개수만큼 반복
    when {{ vendor_id }} then '{{ vendor_name }}'
    {% endfor %}
end

{% endmacro %}
