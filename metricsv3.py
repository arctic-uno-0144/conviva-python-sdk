"""
A simple Python wrapper for interacting with the Conviva API.

https://developer.conviva.com/
https://developer.conviva.com/docs/overview/b36bd03b81f71-conviva-video-ap-is-overview
https://developer.conviva.com/docs/metrics-api-v3/7ae5442782284-getting-started


:copyright: (c) 2024 Shayne Reese.
:version: 0.0.1
"""
import re
import requests


# https://developer.conviva.com/docs/metrics-api-v3/84ff2dc99dfeb-metrics
METRICS = (
    "ad-actual-duration", "ad-attempts", "ad-bitrate",
    "ad-completed-creative-plays", "ad-concurrent-plays",
    "ad-connection-induced-rebuffering-ratio", "ad-ended-plays",
    "exits-before-ad-start", "ad-framerate", "ad-minutes-played",
    "ad-percentage-complete", "ad-impressions", "ad-rebuffering-ratio",
    "ad-unique-devices", "ad-video-playback-failures", "ad-video-restart-time",
    "ad-video-start-failures", "ad-video-start-time", "attempts",
    "attempts-with-pre-roll", "attempts-without-pre-roll", "bad-session",
    "bad-session-average-life-playing-time-mins", "bad-unique-devices",
    "bad-unique-viewers", "bitrate", "concurrent-plays",
    "connection-induced-rebuffering-ratio", "ended-plays",
    "ended-plays-with-ads", "ended-plays-without-ads",
    "exits-before-video-start", "framerate", "good-session",
    "good-session-average-life-playing-time-mins", "good-unique-devices",
    "good-unique-viewers", "abandonment", "abandonment-with-pre-roll",
    "abandonment-without-pre-roll", "high-rebuffering", "high-rebuffering-with-ads",
    "high-rebuffering-without-ads", "high-startup-time", "high-startup-time-with-pre-roll",
    "high-startup-time-without-pre-roll", "interval-minutes-played", "low-bitrate",
    "low-bitrate-with-ads", "low-bitrate-without-ads", "minutes-played", "non-zero-cirr-ended-plays",
    "percentage-complete", "plays", "rebuffering-ratio", "spi-streams",
    "spi-unique-devices", "spi-unique-viewers", "streaming-performance-index",
    "unique-devices", "video-playback-failures", "video-playback-failures-business",
    "video-playback-failures-tech", "video-playback-failures-tech-with-ads",
    "video-playback-failures-tech-without-ads", "video-restart-time", "video-start-failures",
    "video-start-failures-business", "video-start-failures-tech", "video-start-failures-tech-with-pre-roll",
    "video-start-failures-tech-without-pre-roll", "video-start-time", "zero-cirr-ended-plays"
)

# https://developer.conviva.com/docs/metrics-api-v3/c0ad2c0d78418-conviva-defined-dimensions#filtering
FILTER_DIMENSIONS = (
    "ad_break_id", "ad_break_index", "ad_campaign_name",
    "ad_category", "ad_creative_id", "ad_creative_name",
    "ad_creative_type", "ad_day_part", "ad_deal_id",
    "ad_fallback_index", "ad_first_ad_id", "ad_first_ad_system",
    "ad_first_creative_id", "ad_id", "ad_is_slate",
    "ad_manager_name", "ad_manager_version", "ad_media_file_api_framework",
    "ad_planned_duration", "ad_position", "ad_sequence",
    "ad_session_start_event", "ad_stitcher", "ad_system",
    "ad_technology", "ad_type", "ad_unit_name",
    "ad_video_asset_name", "advertiser", "advertiser_category",
    "advertiser_id", "advertiser_name", "app_version",
    "asn", "asset", "asset_type",
    "browser_name", "browser_version", "cdn",
    "cdn_edge_group", "cdn_edge_server", "connection_type",
    "content_category", "content_meta_affiliate", "content_meta_brand",
    "content_meta_category_type", "content_meta_channel", "content_meta_content_type",
    "content_meta_episode_number", "content_meta_genre", "content_meta_genre_list",
    "content_meta_id", "content_meta_name", "content_meta_season_number",
    "content_meta_series_name", "content_meta_show_title", "conviva_core_sdk_version",
    "customer", "device_category", "device_hardware_type", "device_manufacturer",
    "device_marketing_name", "device_model", "device_name",
    "device_os", "device_os_family", "device_os_version",
    "geo_city_name", "geo_continent_name", "geo_country_code",
    "geo_dma", "geo_postal_code", "geo_state_name",
    "is_ad_requested", "isp", "network_connection_type",
    "platform", "platform_version", "player_name",
    "player_framework_name", "player_framework_version", "precision_algo_id",
    "precision_cdn", "precision_rs", "preroll_status",
    "stream_host", "user_agent", "utm_tracking_code",
    "video_asset_name", "video_playback_failure_business_error",
    "video_playback_failure_error", "video_playback_failure_tech_error",
    "video_start_failure_business_error", "video_start_failure_error",
    "video_start_failure_tech_error"
)

# https://developer.conviva.com/docs/metrics-api-v3/c0ad2c0d78418-conviva-defined-dimensions#group-by
GROUP_BY_DIMENSIONS = (
    "ad-break-id", "ad-break-index", "ad-campaign-name",
    "ad-category", "ad-creative-id", "ad-creative-name",
    "ad-creative-type", "ad-day-part", "ad-deal-id",
    "ad-fallback-index", "ad-first-ad-id", "ad-first-ad-system",
    "ad-first-creative-id", "ad-id", "ad-is-slate",
    "ad-manager-name", "ad-manager-version", "ad-media-file-api-framework",
    "ad-planned-duration", "ad-position", "ad-sequence",
    "ad-session-start-event", "ad-stitcher", "ad-system",
    "ad-technology", "ad-type", "ad-unit-name",
    "ad-video-asset-name", "advertiser", "advertiser-category",
    "advertiser-id", "advertiser-name", "app-version",
    "asn", "asset", "asset-type",
    "browser-name", "browser-version", "cdn",
    "cdn-edge-group", "cdn-edge-server", "connection-type",
    "content-category", "content-meta-affiliate", "content-meta-brand",
    "content-meta-category-type", "content-meta-channel", "content-meta-content-type",
    "content-meta-episode-number", "content-meta-genre", "content-meta-genre-list",
    "content-meta-id", "content-meta-name", "content-meta-season-number",
    "content-meta-series-name", "content-meta-show-title", "conviva-core-sdk-version",
    "customer", "device-category", "device-hardware-type",
    "device-manufacturer", "device-marketing-name", "device-model",
    "device-name", "device-os", "device-os-family",
    "device-os-version", "geo-city-name", "geo-continent-name",
    "geo-country-code", "geo-dma", "geo-postal-code",
    "geo-state-name", "is-ad-requested", "isp",
    "network-connection-type", "platform", "platform-version",
    "player-name", "player-framework-name", "player-framework-version",
    "precision-algo-id", "precision-cdn", "precision-rs",
    "preroll-status", "stream-host", "user-agent",
    "utm-tracking-code", "video-asset-name", "video-playback-failure-error",
    "video-playback-failure-business-error", "video-playback-failure-tech-error",
    "video-start-failure-error", "video-start-failure-business-error",
    "video-start-failure-tech-error", "ad-video-start-failure-error",
    "ad-video-playback-failure-error"
)

# https://developer.conviva.com/docs/metrics-api-v3/66fa07fe366d2-granularity-options
TIME_PERIODS = (
    "PT1M", "PT2M", "PT3M", "PT4M", "PT5M", "PT6M", "PT10M", "PT12M",
    "PT15M", "PT20M", "PT30M", "PT1H", "PT2H", "PT3H", "PT4H", "PT5H",
    "PT6H", "PT7H", "PT8H", "PT9H", "PT10H", "PT11H", "PT12H", "PT13H",
    "PT14H", "PT15H", "PT16H", "PT17H", "PT18H", "PT19H", "PT20H", "PT21H",
    "PT22H", "PT23H", "P1D", "P2D", "P3D", "P4D", "P5D", "P6D", "P7D",
    "P8D", "P9D", "P10D", "P11D", "P12D", "P13D", "P14D", "P15D", "P16D",
    "P17D", "P18D", "P19D", "P20D", "P21D", "P22D", "P23D", "P24D", "P25D",
    "P26D", "P27D", "P28D", "P29D", "P30D", "P1W", "P2W", "P3W", "P4W",
    "P5W", "P6W", "P7W", "P8W", "P9W", "P10W", "ALL"
)

# ISO-8601 formated string
# https://developer.conviva.com/docs/metrics-api-v3/3434cc866b1a9-options-to-select-a-time-range
ISO_TIME = re.compile(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[A-Z]{1,4}')


# Metrics V3 Client
class MetricsV3:

    def __init__(self, api_key: str = None) -> None:
        """ Wrapper for the Conviva Metrics V3 API
            https://developer.conviva.com/docs/metrics-api-v3/7ae5442782284-getting-started
        """
        super().__init__()
        self.url = "https://api.conviva.com/insights/3.0"
        self.mock = "https://stoplight.io/mocks/conviva/metrics-api-v3/42579808"
        self.key = api_key

    def get_metric(
            self,
            live: bool = None,
            metric: str = None,
            custom_selection: list = None,
            group_by: str = None,
            mock: str = None,
            days: int = None,
            minutes: int = None,
            granularity: str = None,
            no_nils: bool = None,
            start_date: str = None,
            start_epoch: int = None,
            start_epoch_ms: int = None,
            end_date: str = None,
            end_epoch: int = None,
            end_epoch_ms: int = None,
            filter_id: str = None,
            kpi_id: str = None,
            tag: str = None,
            dimension: dict = None) -> dict:
        """
        Make a GET request to the Conviva Metrics v3 API.

        :param bool live: Use the 'real-time' or 'metrics' API path.
        :param str metric: Conviva MetricsV3 metric to query.
        :param list custom_selection: List of Conviva MetricsV3 metrics to query.
        :param str group_by: Conviva dimension to group query results by. (see: https://developer.conviva.com/docs/metrics-api-v3/c0ad2c0d78418-conviva-defined-dimensions).
        :param bool mock: Use a mock server or production server url.
        :param int days: Number of previous days from the most recent clock hour. [default=1]
        :param int minutes: Time range to retrieve real-time metrics. [default=5] [max=15]
        :param str granularity: ISO-8601 duration format.  [default='PT1H']
        :param bool no_nils: Filter out time series data points with all zero values.
        :param str start_date: ISO-8601 format date. (ex. 'YYYY-MM-DDTHH:MM:SSZ')
        :param int start_epoch: Unix epoch time in seconds.
        :param int start_epoch_ms: Unix epoch time in milliseconds.
        :param str end_date: ISO-8601 format date. (ex. 'YYYY-MM-DDTHH:MM:SSZ')
        :param int end_epoch: Unix epoch time in seconds.
        :param int end_epoch_ms: Unix epoch time in milliseconds.
        :param str filter_id: Id of a saved filter (cannot be used in conjunction with other dimensional filters).
        :param str kpi_id: Id of KPI thresholds; by default (if omitted) the value is assumed to be kpi_id=1, i.e. Conviva Good.
        :param str tag: Filter by custom dimension tag <tag_{customTagName}> (where tag_ is a prefix and {customTagName} is a placeholder of your tag name).
        :param dict dimension: Filter by a dimension label (see: https://developer.conviva.com/docs/metrics-api-v3/c0ad2c0d78418-conviva-defined-dimensions).
        """
        default_response = {
            "url": "",
            "headers": {},
            "status_code": 0,
            "reason": "ERROR",
            "text": "",
            "json": {}
        }
        # set the base request url
        r_url = self.mock if mock else self.url
        # determine 'real-time' or 'historical'
        r_url += "/real-time-metrics" if live else "/metrics"
        # init param dict
        r_params: dict = {}
        # singular metric or list of custom selections?
        if metric:
            if metric not in METRICS:
                print(
                    "'metric' Must be a valid Conviva Defined Metric paramter ( see: "
                    "https://developer.conviva.com/docs/metrics-api-v3/84ff2dc99dfeb-metrics )")
                return default_response
            # add parameter
            r_url += f"/{metric}"
            # check dimension grouping
            if group_by:
                if group_by in GROUP_BY_DIMENSIONS:
                    r_url += f"/group-by/{group_by}"
                else:
                    print("'group_by' Must be a valid Conviva Definited Dimension ( see: "
                          "https://developer.conviva.com/docs/metrics-api-v3/c0ad2c0d78418-conviva-defined-dimensions#group-by )")
            # check the default args
            if live:
                r_params["minutes"] = minutes
            else:
                if granularity:
                    if granularity not in TIME_PERIODS:
                        r_params["granularity"] = "PT1H"
                        print("'granularity' Must be a valid ISO-8601 string"
                              " [using default 'PT1H'] ( see: "
                              "https://developer.conviva.com/docs/metrics-api-v3/66fa07fe366d2-granularity-options )")
                if days:
                    if int(days) < 1:
                        days = 1
                    r_params["days"] = days
        elif custom_selection:
            # add custom selection param
            r_url += "/custom-selection?"
            # add each metric to the url
            for i, selection in enumerate(custom_selection):
                if selection not in METRICS:
                    print(
                        "'metric' Must be a valid Conviva Defined Metric paramter ( see: "
                        "https://developer.conviva.com/docs/metrics-api-v3/84ff2dc99dfeb-metrics )")
                    continue
                elif i+1 == len(custom_selection):
                    r_url += f"metric={selection}"
                else:
                    r_url += f"metric={selection}&"
            # this requires start_date and end_date
            if not start_date or not end_date:
                print("'custom_selection' requires the 'start_date' and 'end_date' parameters!")
                return default_response
        else:
            print(
                "No 'metric'(s) selected, enter a singular "
                "'metric' or 'custom_selection' list!")
            return default_response
        # check for start/end dates
        if start_date:
            # verify ISO formatting
            if ISO_TIME.fullmatch(start_date):
                r_params["start_date"] = start_date
            else:
                print("'start_date' must be in ISO-8601 format 'YYYY-MM-DDTHH:MM:SSZ' ( see: "
                      "https://developer.conviva.com/docs/metrics-api-v3/3434cc866b1a9-options-to-select-a-time-range )")
        if end_date:
            # verify ISO formatting
            if ISO_TIME.fullmatch(end_date):
                r_params["end_date"] = end_date
            else:
                print("'end_date' must be in ISO-8601 format 'YYYY-MM-DDTHH:MM:SSZ' ( see: "
                      "https://developer.conviva.com/docs/metrics-api-v3/3434cc866b1a9-options-to-select-a-time-range )")
        # check optional params
        opts: dict = {
            "start_epoch": start_epoch,
            "start_epoch_ms": start_epoch_ms,
            "end_epoch": start_epoch,
            "end_epoch_ms": start_epoch_ms,
            "kpi_id": kpi_id
        }
        opts["no_nils"] = "true" if no_nils else None
        if tag:
            # validate tag
            try:
                _tag = tag.split("=")
                tag_name = f"tag_{_tag[0]}"
                opts[tag_name] = _tag[1]
            except Exception:
                print("'tag' Must be a string of format 'tagName=tagValue")
        # check filter dimensions
        if filter_id:
            opts["filter_id"] = filter_id
        elif dimension:
            # mutex; 'filter_id' is a saved dimension filter
            if isinstance(dimension, dict) and dimension.get('param'):
                if dimension['param'] not in FILTER_DIMENSIONS:
                    print(
                        "'dimension' must be a vaild Conviva Defined Dimension ( see: "
                        "https://developer.conviva.com/docs/metrics-api-v3/c0ad2c0d78418-conviva-defined-dimensions#filtering )")
                else:
                    opts[dimension["param"]] = dimension["value"]
            else:
                print("'dimension' must be a dictionary in the format\n"
                      "{'param': '<conviva-defined-dimension>', 'value': '<dimension-filter-value>'}")
        # update params
        for k, v in opts.items():
            if v is not None:
                r_params[k] = v
        # set request headers
        r_headers = {"Accept": "application/json"}
        # add authentication header if present
        if not mock:
            if self.key:
                r_headers.update({"Authorization": self.key})
            else:
                print(
                    "Warning: No API Key set for request to production server, "
                    "add an API Key, or set 'mock'=True to use a mock server")
        # make the request
        res = requests.get(r_url, headers=r_headers, params=r_params, timeout=20)
        # process the response
        if isinstance(res, requests.models.Response):
            response = {
                "url": res.url,
                "headers": res.headers,
                "status_code": res.status_code,
                "reason": res.reason,
                "text": res.text
            }
            response["json"] = res.json() if hasattr(res, "json") else {}
            return response
        else:
            print(
                "Invalid Response Object, check the request!\n"
                f"r_url: {r_url}\nr_headers: {r_headers}")
        return default_response
