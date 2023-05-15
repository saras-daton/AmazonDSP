{% if var('AmazonDSPProductsReport') %}
{{ config( enabled = True ) }}
{% else %}
{{ config( enabled = False ) }}
{% endif %}


{% if is_incremental() %}
{%- set max_loaded_query -%}
SELECT coalesce(MAX(_daton_batch_runtime) - 2592000000,0) FROM {{ this }}
{% endset %}

{%- set max_loaded_results = run_query(max_loaded_query) -%}

{%- if execute -%}
{% set max_loaded = max_loaded_results.rows[0].values()[0] %}
{% else %}
{% set max_loaded = 0 %}
{%- endif -%}
{% endif %}


{% set table_name_query %}
{{set_table_name('%amazondsp%productsreport')}}    
{% endset %}  

{% set results = run_query(table_name_query) %}
{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% set tables_lowercase_list = results.columns[1].values() %}
{% else %}
{% set results_list = [] %}
{% set tables_lowercase_list = [] %}
{% endif %}

{% for i in results_list %}
    {% if var('get_brandname_from_tablename_flag') %}
        {% set brand =i.split('.')[2].split('_')[var('brandname_position_in_tablename')] %}
    {% else %}
        {% set brand = var('default_brandname') %}
    {% endif %}

    {% if var('get_storename_from_tablename_flag') %}
        {% set store =i.split('.')[2].split('_')[var('storename_position_in_tablename')] %}
    {% else %}
        {% set store = var('default_storename') %}
    {% endif %}

    {% if var('timezone_conversion_flag') and i.lower() in tables_lowercase_list and i in var('raw_table_timezone_offset_hours')%}
        {% set hr = var('raw_table_timezone_offset_hours')[i] %}
    {% else %}
        {% set hr = 0 %}
    {% endif %}

    SELECT * {{exclude()}} (row_num)
    From (
        select 
        '{{brand}}' as brand,
        '{{store}}' as store,
        profileId,
        countryName,
        accountName,
        accountId,
        reportDate,
        date,
        advertiserName,
        entityId,
        advertiserId,
        reportGranularity,
        amazonStandardId,
        parentASIN,
        marketplace,
        brandName,
        asinConversionType,
        featuredASIN,
        productName,
        productGroup,
        productCategory,
        productSubcategory,
        dpv14d,
        dpvViews14d,
        dpvClicks14d,
        pRPV14d,
        pRPVViews14d,
        pRPVClicks14d,
        atl14d,
        atlViews14d,
        atlClicks14d,
        atc14d,
        atcViews14d,
        atcClicks14d,
        purchases14d,
        purchasesViews14d,
        purchasesClicks14d,
        newToBrandPurchases14d,
        newToBrandPurchasesViews14d,
        newToBrandPurchasesClicks14d,
        percentOfPurchasesNewToBrand14d,
        totalDetailPageViews14d,
        totalDetailPageViewViews14d,
        totalDetailPageClicks14d,
        totalPRPV14d,
        totalPRPVViews14d,
        totalPRPVClicks14d,
        totalAddToList14d,
        totalAddToListViews14d,
        totalAddToListClicks14d,
        totalAddToCart14d,
        totalAddToCartViews14d,
        totalAddToCartClicks14d,
        totalPurchases14d,
        totalPurchasesViews14d,
        totalPurchasesClicks14d,
        totalNewToBrandPurchases14d,
        totalNewToBrandPurchasesViews14d,
        totalNewToBrandPurchasesClicks14d,
        totalPercentOfPurchasesNewToBrand14d,
        newSubscribeAndSave14d,
        newSubscribeAndSaveViews14d,
        newSubscribeAndSaveClicks14d,
        totalSubscribeAndSaveSubscriptions14d,
        totalSubscribeAndSaveSubscriptionViews14d,
        totalSubscribeAndSaveSubscriptionClicks14d,
        unitsSold14d,
        sales14d,
        totalUnitsSold14d,
        totalSales14d,
        newToBrandUnitsSold14d,
        newToBrandProductSales14d,
        brandHaloDetailPage14d,
        brandHaloDetailPageViews14d,
        brandHaloDetailPageClicks14d,
        brandHaloProductReviewPage14d,
        brandHaloProductReviewPageViews14d,
        brandHaloProductReviewPageClicks14d,
        brandHaloAddToList14d,
        brandHaloAddToListViews14d,
        brandHaloAddToListClicks14d,
        brandHaloAddToCart14d,
        brandHaloAddToCartViews14d,
        brandHaloAddToCartClicks14d,
        brandHaloPurchases14d,
        brandHaloPurchasesViews14d,
        brandHaloPurchasesClicks14d,
        brandHaloNewToBrandPurchases14d,
        brandHaloNewToBrandPurchasesViews14d,
        brandHaloNewToBrandPurchasesClicks14d,
        brandHaloPercentOfPurchasesNewToBrand14d,
        brandHaloNewSubscribeAndSave14d,
        brandHaloNewSubscribeAndSaveViews14d,
        brandHaloNewSubscribeAndSaveClicks14d,
        brandHaloTotalUnitsSold14d,
        brandHaloTotalSales14d,
        brandHaloTotalNewToBrandSales14d,
        brandHaloTotalNewToBrandUnitsSold14d,
        orderName,
        orderId,
        orderStartDate,
        orderEndDate,
        orderBudget,
        orderExternalId,
        orderCurrency,
        lineItemName,
        lineItemId,
        lineItemStartDate,
        lineItemEndDate,
        lineItemBudget,
        lineItemExternalId,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (PARTITION BY advertiserId,ReportDate,OrderId,LineItemId order by {{daton_batch_runtime()}} desc) row_num
        from {{i}} 
                    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}        
    )
    where row_num = 1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}
