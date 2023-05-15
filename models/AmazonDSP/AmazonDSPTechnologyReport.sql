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
    {{set_table_name('%amazondsp%technologyreport')}}    
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
            browser,
            agencyFee,
            totalCost,
            impressions,
            viewableImpressions,
            clickThroughs,
            totalPixel14d,
            totalPixelViews14d,
            totalPixelClicks14d,
            marketingLandingPage14d,
            marketingLandingPageViews14d,
            marketingLandingPageClicks14d,
            subscriptionPage14d,
            subscriptionPageViews14d,
            subscriptionPageClicks14d,
            signUpPage14d,
            signUpPageViews14d,
            signUpPageClicks14d,
            application14d,
            applicationViews14d,
            applicationClicks14d,
            gameLoad14d,
            gameLoadViews14d,
            gameLoadClicks14d,
            widgetLoad14d,
            widgetLoadViews14d,
            widgetLoadClicks14d,
            surveyStart14d,
            surveyStartViews14d,
            surveyStartClicks14d,
            surveyFinish14d,
            surveyFinishViews14d,
            surveyFinishClicks14d,
            bannerInteraction14d,
            bannerInteractionViews14d,
            bannerInteractionClicks14d,
            widgetInteraction14d,
            widgetInteractionViews14d,
            widgetInteractionClicks14d,
            gameInteraction14d,
            gameInteractionViews14d,
            gameInteractionClicks14d,
            emailLoad14d,
            emailLoadViews14d,
            emailLoadClicks14d,
            emailInteraction14d,
            emailInteractionViews14d,
            emailInteractionClicks14d,
            submitButton14d,
            submitButtonViews14d,
            submitButtonClicks14d,
            purchaseButton14d,
            purchaseButtonViews14d,
            purchaseButtonClicks14d,
            clickOnRedirect14d,
            clickOnRedirectViews14d,
            clickOnRedirectClicks14d,
            dropDownSelection14d,
            dropDownSelectionViews14d,
            dropDownSelectionClicks14d,
            signUpButton14d,
            signUpButtonViews14d,
            signUpButtonClicks14d,
            subscriptionButton14d,
            subscriptionButtonViews14d,
            subscriptionButtonClicks14d,
            successPage14d,
            successPageViews14d,
            successPageClicks14d,
            thankYouPage14d,
            thankYouPageViews14d,
            thankYouPageClicks14d,
            registrationForm14d,
            registrationFormViews14d,
            registrationFormClicks14d,
            registrationConfirmPage14d,
            registrationConfirmPageViews14d,
            registrationConfirmPageClicks14d,
            storeLocatorPage14d,
            storeLocatorPageViews14d,
            storeLocatorPageClicks14d,
            mobileAppFirstStarts14d,
            mobileAppFirstStartViews14d,
            mobileAppFirstStartClicks14d,
            brandStoreEngagement1,
            brandStoreEngagement1Views,
            brandStoreEngagement1Clicks,
            brandStoreEngagement2,
            brandStoreEngagement2Views,
            brandStoreEngagement2Clicks,
            brandStoreEngagement3,
            brandStoreEngagement3Views,
            brandStoreEngagement3Clicks,
            brandStoreEngagement4,
            brandStoreEngagement4Views,
            brandStoreEngagement4Clicks,
            brandStoreEngagement5,
            brandStoreEngagement5Views,
            brandStoreEngagement5Clicks,
            brandStoreEngagement6,
            brandStoreEngagement6Views,
            brandStoreEngagement6Clicks,
            brandStoreEngagement7,
            brandStoreEngagement7Views,
            brandStoreEngagement7Clicks,
            addedToShoppingCart14d,
            addedToShoppingCartViews14d,
            addedToShoppingCartClicks14d,
            productPurchased,
            productPurchasedViews,
            productPurchasedClicks,
            homepageVisit14d,
            homepageVisitViews14d,
            homepageVisitClicks14d,
            videoStarted,
            videoStartedViews,
            videoStartedClicks,
            videoCompleted,
            videoCompletedViews,
            videoEndClicks,
            messageSent14d,
            messageSentViews14d,
            messageSentClicks14d,
            mashupClickToPage,
            mashupClickToPageViews,
            mashupClickToPageClicks,
            mashupBackupImage,
            mashupBackupImageViews,
            mashupBackupImageClicks,
            mashupAddToCart14d,
            mashupAddToCartViews14d,
            mashupAddToCartClicks14d,
            mashupAddToWishlist14d,
            mashupAddToWishlistViews14d,
            mashupAddToWishlistClicks14d,
            mashupSubscribeAndSave14d,
            mashupSubscribeAndSaveClickViews14d,
            mashupSubscribeAndSaveClick14d,
            mashupClipCouponClick14d,
            mashupClipCouponClickViews14d,
            mashupClipCouponClickClicks14d,
            mashupShopNowClick14d,
            mashupShopNowClickViews14d,
            mashupShopNowClickClicks14d,
            referral14d,
            referralViews14d,
            referralClicks14d,
            accept14d,
            acceptViews14d,
            acceptClicks14d,
            decline14d,
            declineViews14d,
            declineClicks14d,
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
            newSubscribeAndSave14d,
            newSubscribeAndSaveViews14d,
            newSubscribeAndSaveClicks14d,
            addToWatchlist14d,
            addToWatchlistViews14d,
            addToWatchlistClicks14d,
            downloadedVideoPlays14d,
            downloadedVideoPlaysViews14d,
            downloadedVideoPlaysClicks14d,
            videoStreams14d,
            videoStreamsViews14d,
            videoStreamsClicks14d,
            playTrailers14d,
            playTrailersViews14d,
            playerTrailersClicks14d,
            rentals14d,
            rentalsViews14d,
            rentalsClicks14d,
            videoDownloads14d,
            videoDownloadsViews14d,
            videoDownloadsClicks14d,
            videoStart,
            videoFirstQuartile,
            videoMidpoint,
            videoThirdQuartile,
            videoComplete,
            videoPause,
            videoResume,
            videoMute,
            videoUnmute,
            unitsSold14d,
            sales14d,
            newToBrandUnitsSold14d,
            newToBrandProductSales14d,
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
            operatingSystem,
            browserVersion,
            device,
            {{daton_user_id()}} as _daton_user_id,
            {{daton_batch_runtime()}} as _daton_batch_runtime,
            {{daton_batch_id()}} as _daton_batch_id,
            current_timestamp() as _last_updated,
            '{{env_var("DBT_CLOUD_RUN_ID", "manual")}}' as _run_id,
            ROW_NUMBER() OVER (PARTITION BY ReportDate,OrderId,LineItemId  order by {{daton_batch_runtime()}} desc) row_num
            from {{i}} 
                        {% if is_incremental() %}
                {# /* -- this filter will only be applied on an incremental run */ #}
                WHERE {{daton_batch_runtime()}}  >= {{max_loaded}}
                {% endif %}        
        )
        where row_num = 1 
        {% if not loop.last %} union all {% endif %}
    {% endfor %}