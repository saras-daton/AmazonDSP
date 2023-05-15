{% if var('AmazonDSPCampaignReport') %}
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
{{set_table_name('%amazondsp%campaignreport')}}    
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
        entityId,
        date,
        advertiserName,
        advertiserId,
        totalCost,
        supplyCost,
        amazonAudienceFee,
        advertiserTimezone,
        advertiserCountry,
        amazonPlatformFee,
        impressions,
        clickThroughs,
        CTR,
        eCPM,
        eCPC,
        dpv14d,
        dpvViews14d,
        dpvClicks14d,
        dpvr14d,
        eCPDPV14d,
        pRPV14d,
        pRPVViews14d,
        pRPVClicks14d,
        pRPVr14d,
        eCPPRPV14d,
        atl14d,
        atlViews14d,
        atlClicks14d,
        atlr14d,
        eCPAtl14d,
        atc14d,
        atcViews14d,
        atcClicks14d,
        atcr14d,
        eCPAtc14d,
        purchases14d,
        purchasesViews14d,
        purchasesClicks14d,
        purchaseRate14d,
        eCPP14d,
        newToBrandPurchases14d,
        newToBrandPurchasesViews14d,
        newToBrandPurchasesClicks14d,
        newToBrandPurchaseRate14d,
        newToBrandECPP14d,
        percentOfPurchasesNewToBrand14d,
        addToWatchlist14d,
        addToWatchlistViews14d,
        addToWatchlistClicks14d,
        addToWatchlistCVR14d,
        addToWatchlistCPA14d,
        downloadedVideoPlays14d,
        downloadedVideoPlaysViews14d,
        downloadedVideoPlaysClicks14d,
        downloadedVideoPlayRate14d,
        eCPDVP14d,
        videoStreams14d,
        videoStreamsViews14d,
        videoStreamsClicks14d,
        videoStreamsRate14d,
        eCPVS14d,
        playTrailers14d,
        playTrailersViews14d,
        playerTrailersClicks14d,
        playTrailerRate14d,
        eCPPT14d,
        rentals14d,
        rentalsViews14d,
        rentalsClicks14d,
        rentalRate14d,
        ecpr14d,
        videoDownloads14d,
        videoDownloadsViews14d,
        videoDownloadsClicks14d,
        videoDownloadRate14d,
        ecpvd14d,
        newSubscribeAndSave14d,
        newSubscribeAndSaveViews14d,
        newSubscribeAndSaveClicks14d,
        newSubscribeAndSaveRate14d,
        eCPnewSubscribeAndSave14d,
        totalPixel14d,
        totalPixelViews14d,
        totalPixelClicks14d,
        totalPixelCVR14d,
        totalPixelCPA14d,
        marketingLandingPage14d,
        marketingLandingPageViews14d,
        marketingLandingPageClicks14d,
        marketingLandingPageCVR14d,
        marketingLandingPageCPA14d,
        subscriptionPage14d,
        subscriptionPageViews14d,
        subscriptionPageClicks14d,
        subscriptionPageCVR14d,
        subscriptionPageCPA14d,
        signUpPage14d,
        signUpPageViews14d,
        signUpPageClicks14d,
        signUpPageCVR14d,
        signUpPageCPA14d,
        application14d,
        applicationViews14d,
        applicationClicks14d,
        applicationCVR14d,
        applicationCPA14d,
        gameLoad14d,
        gameLoadViews14d,
        gameLoadClicks14d,
        gameLoadCVR14d,
        gameLoadCPA14d,
        widgetLoad14d,
        widgetLoadViews14d,
        widgetLoadClicks14d,
        widgetLoadCVR14d,
        widgetLoadCPA14d,
        surveyStart14d,
        surveyStartViews14d,
        surveyStartClicks14d,
        surveyStartCVR14d,
        surveyStartCPA14d,
        surveyFinish14d,
        surveyFinishViews14d,
        surveyFinishClicks14d,
        surveyFinishCVR14d,
        surveyFinishCPA14d,
        bannerInteraction14d,
        bannerInteractionViews14d,
        bannerInteractionClicks14d,
        bannerInteractionCVR14d,
        bannerInteractionCPA14d,
        widgetInteraction14d,
        widgetInteractionViews14d,
        widgetInteractionClicks14d,
        widgetInteractionCVR14d,
        widgetInteractionCPA14d,
        gameInteraction14d,
        gameInteractionViews14d,
        gameInteractionClicks14d,
        gameInteractionCVR14d,
        gameInteractionCPA14d,
        emailLoad14d,
        emailLoadViews14d,
        emailLoadClicks14d,
        emailLoadCVR14d,
        emailLoadCPA14d,
        emailInteraction14d,
        emailInteractionViews14d,
        emailInteractionClicks14d,
        emailInteractionCVR14d,
        emailInteractionCPA14d,
        submitButton14d,
        submitButtonViews14d,
        submitButtonClicks14d,
        submitButtonCVR14d,
        submitButtonCPA14d,
        purchaseButton14d,
        purchaseButtonViews14d,
        purchaseButtonClicks14d,
        purchaseButtonCVR14d,
        purchaseButtonCPA14d,
        clickOnRedirect14d,
        clickOnRedirectViews14d,
        clickOnRedirectClicks14d,
        clickOnRedirectCVR14d,
        clickOnRedirectCPA14d,
        signUpButton14d,
        signUpButtonViews14d,
        signUpButtonClicks14d,
        signUpButtonCVR14d,
        signUpButtonCPA14d,
        subscriptionButton14d,
        subscriptionButtonViews14d,
        subscriptionButtonClicks14d,
        subscriptionButtonCVR14d,
        subscriptionButtonCPA14d,
        successPage14d,
        successPageViews14d,
        successPageClicks14d,
        successPageCVR14d,
        successPageCPA14d,
        thankYouPage14d,
        thankYouPageViews14d,
        thankYouPageClicks14d,
        thankYouPageCVR14d,
        thankYouPageCPA14d,
        registrationForm14d,
        registrationFormViews14d,
        registrationFormClicks14d,
        registrationFormCVR14d,
        registrationFormCPA14d,
        registrationConfirmPage14d,
        registrationConfirmPageViews14d,
        registrationConfirmPageClicks14d,
        registrationConfirmPageCVR14d,
        registrationConfirmPageCPA14d,
        storeLocatorPage14d,
        storeLocatorPageViews14d,
        storeLocatorPageClicks14d,
        storeLocatorPageCVR14d,
        storeLocatorPageCPA14d,
        mobileAppFirstStarts14d,
        mobileAppFirstStartViews14d,
        mobileAppFirstStartClicks14d,
        mobileAppFirstStartCVR14d,
        mobileAppFirstStartsCPA14d,
        brandStoreEngagement1,
        brandStoreEngagement1Views,
        brandStoreEngagement1Clicks,
        brandStoreEngagement1CVR,
        brandStoreEngagement1CPA,
        brandStoreEngagement2,
        brandStoreEngagement2Views,
        brandStoreEngagement2Clicks,
        brandStoreEngagement2CVR,
        brandStoreEngagement2CPA,
        brandStoreEngagement3,
        brandStoreEngagement3Views,
        brandStoreEngagement3Clicks,
        brandStoreEngagement3CVR,
        brandStoreEngagement3CPA,
        brandStoreEngagement4,
        brandStoreEngagement4Views,
        brandStoreEngagement4Clicks,
        brandStoreEngagement4CVR,
        brandStoreEngagement4CPA,
        brandStoreEngagement5,
        brandStoreEngagement5Views,
        brandStoreEngagement5Clicks,
        brandStoreEngagement5CVR,
        brandStoreEngagement5CPA,
        brandStoreEngagement6,
        brandStoreEngagement6Views,
        brandStoreEngagement6Clicks,
        brandStoreEngagement6CVR,
        brandStoreEngagement6CPA,
        brandStoreEngagement7,
        brandStoreEngagement7Views,
        brandStoreEngagement7Clicks,
        brandStoreEngagement7CVR,
        brandStoreEngagement7CPA,
        addedToShoppingCart14d,
        addedToShoppingCartViews14d,
        addedToShoppingCartClicks14d,
        addedToShoppingCartCVR14d,
        addedToShoppingCartCPA14d,
        productPurchased,
        productPurchasedViews,
        productPurchasedClicks,
        productPurchasedCVR,
        productPurchasedCPA,
        homepageVisit14d,
        homepageVisitViews14d,
        homepageVisitClicks14d,
        homepageVisitCVR14d,
        homepageVisitCPA14d,
        videoStarted,
        videoStartedViews,
        videoStartedClicks,
        videoStartedCVR,
        videoStartedCPA,
        videoCompleted,
        videoCompletedViews,
        videoEndClicks,
        videoCompletedCVR,
        videoCompletedCPA,
        messageSent14d,
        messageSentViews14d,
        messageSentClicks14d,
        messageSentCVR14d,
        messageSentCPA14d,
        mashupClickToPage,
        mashupClickToPageViews,
        mashupClickToPageClicks,
        mashupClickToPageCVR,
        mashupClickToPageCPA,
        mashupBackupImage,
        mashupBackupImageViews,
        mashupBackupImageClicks,
        mashupBackupImageCVR,
        mashupBackupImageCPA,
        mashupAddToCart14d,
        mashupAddToCartViews14d,
        mashupAddToCartClicks14d,
        mashupAddToCartClickCVR14d,
        mashupAddToCartCPA14d,
        mashupAddToWishlist14d,
        mashupAddToWishlistViews14d,
        mashupAddToWishlistClicks14d,
        mashupAddToWishlistCVR14d,
        mashupAddToWishlistCPA14d,
        mashupSubscribeAndSave14d,
        mashupSubscribeAndSaveClickViews14d,
        mashupSubscribeAndSaveClick14d,
        mashupSubscribeAndSaveCVR14d,
        mashupSubscribeAndSaveCPA14d,
        mashupClipCouponClick14d,
        mashupClipCouponClickViews14d,
        mashupClipCouponClickClicks14d,
        mashupClipCouponClickCVR14d,
        mashupClipCouponClickCPA14d,
        mashupShopNowClick14d,
        mashupShopNowClickViews14d,
        mashupShopNowClickClicks14d,
        mashupShopNowClickCVR14d,
        mashupShopNowClickCPA14d,
        referral14d,
        referralViews14d,
        referralClicks14d,
        referralCVR14d,
        referralCPA14d,
        accept14d,
        acceptViews14d,
        acceptClicks14d,
        acceptCVR14d,
        acceptCPA14d,
        decline14d,
        declineViews14d,
        declineClicks14d,
        declineCVR14d,
        declineCPA14d,
        videoStart,
        videoFirstQuartile,
        videoMidpoint,
        videoThirdQuartile,
        videoComplete,
        videoCompletionRate,
        ecpvc,
        videoPause,
        videoResume,
        videoMute,
        videoUnmute,
        dropDownSelection14d,
        dropDownSelectionViews14d,
        dropDownSelectionClicks14d,
        dropDownSelectionCVR14d,
        dropDownSelectionCPA14d,
        brandSearch14d,
        brandSearchViews14d,
        brandSearchClicks14d,
        brandSearchRate14d,
        brandSearchCPA14d,
        grossImpressions,
        grossClickThroughs,
        invalidImpressions,
        invalidClickThroughs,
        invalidImpressionRate,
        invalidClickThroughsRate,
        agencyFee,
        totalFee,
        {% if target.type=='snowflake' %} 
            daton_pre_3pFeeAutomotive,
            daton_pre_3pFeeAutomotiveAbsorbed,
            daton_pre_3pFeeComScore,
            daton_pre_3pFeeComScoreAbsorbed,
            daton_pre_3pFeeCPM1,
            daton_pre_3pFeeCPM1Absorbed,
            daton_pre_3pFeeCPM2,
            daton_pre_3pFeeCPM2Absorbed,
            daton_pre_3pFeeCPM3,
            daton_pre_3pFeeCPM3Absorbed,
            daton_pre_3pFeeDoubleclickCampaignManager,
            daton_pre_3pFeeDoubleclickCampaignManagerAbsorbed,
            daton_pre_3pFeeDoubleVerify,
            daton_pre_3pFeeDoubleVerifyAbsorbed,
            daton_pre_3pFeeIntegralAdScience,
            daton_pre_3pFeeIntegralAdScienceAbsorbed,
            daton_pre_3PFees,
        {% else %}
            _daton_pre_3pFeeAutomotive,
            _daton_pre_3pFeeAutomotiveAbsorbed,
            _daton_pre_3pFeeComScore,
            _daton_pre_3pFeeComScoreAbsorbed,
            _daton_pre_3pFeeCPM1,
            _daton_pre_3pFeeCPM1Absorbed,
            _daton_pre_3pFeeCPM2,
            _daton_pre_3pFeeCPM2Absorbed,
            _daton_pre_3pFeeCPM3,
            _daton_pre_3pFeeCPM3Absorbed,
            _daton_pre_3pFeeDoubleclickCampaignManager,
            _daton_pre_3pFeeDoubleclickCampaignManagerAbsorbed,
            _daton_pre_3pFeeDoubleVerify,
            _daton_pre_3pFeeDoubleVerifyAbsorbed,
            _daton_pre_3pFeeIntegralAdScience,
            _daton_pre_3pFeeIntegralAdScienceAbsorbed,
            _daton_pre_3PFees,
        {% endif %}
        unitsSold14d,
        sales14d,
        ROAS14d,
        eRPM14d,
        newToBrandUnitsSold14d,
        newToBrandProductSales14d,
        newToBrandROAS14d,
        newToBrandERPM14d,
        totalPRPV14d,
        totalPRPVViews14d,
        totalPRPVClicks14d,
        totalPRPVr14d,
        totalECPPRPV14d,
        totalPurchases14d,
        totalPurchasesViews14d,
        totalPurchasesClicks14d,
        totalPurchaseRate14d,
        totalECPP14d,
        totalNewToBrandPurchases14d,
        totalNewToBrandPurchasesViews14d,
        totalNewToBrandPurchasesClicks14d,
        totalNewToBrandPurchaseRate14d,
        totalNewToBrandECPP14d,
        totalPercentOfPurchasesNewToBrand14d,
        totalUnitsSold14d,
        totalSales14d,
        totalROAS14d,
        totalERPM14d,
        totalNewToBrandUnitsSold14d,
        totalNewToBrandProductSales14d,
        totalNewToBrandROAS14d,
        totalNewToBrandERPM14d,
        viewableImpressions,
        measurableImpressions,
        measurableRate,
        viewabilityRate,
        totalDetailPageViews14d,
        totalDetailPageViewViews14d,
        totalDetailPageClicks14d,
        totalDetailPageViewsCVR14d,
        totalDetaiPageViewCPA14d,
        totalAddToList14d,
        totalAddToListViews14d,
        totalAddToListClicks14d,
        totalAddToListCVR14d,
        totalAddToListCPA14d,
        totalAddToCart14d,
        totalAddToCartViews14d,
        totalAddToCartClicks14d,
        totalAddToCartCVR14d,
        totalAddToCartCPA14d,
        totalSubscribeAndSaveSubscriptions14d,
        totalSubscribeAndSaveSubscriptionViews14d,
        totalSubscribeAndSaveSubscriptionClicks14d,
        totalSubscribeAndSaveSubscriptionCVR14d,
        totalSubscribeAndSaveSubscriptionCPA14d,
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
        creativeName,
        creativeID,
        creativeType,
        creativeSize,
        creativeAdId,
        {{daton_user_id()}} as _daton_user_id,
        {{daton_batch_runtime()}} as _daton_batch_runtime,
        {{daton_batch_id()}} as _daton_batch_id,
        current_timestamp() as _last_updated,
        '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
        ROW_NUMBER() OVER (PARTITION BY advertiserId,ReportDate,OrderId,LineItemId,CreativeID,CreativeAdId order by {{daton_batch_runtime()}} desc) row_num
        from {{i}} 
                    {% if is_incremental() %}
            {# /* -- this filter will only be applied on an incremental run */ #}
            WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
            {% endif %}        
    )
    where row_num = 1 
    {% if not loop.last %} union all {% endif %}
{% endfor %}
