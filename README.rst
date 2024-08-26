Conviva Python SDK
==================
A simple Python wrapper for the Conviva API.

Currently there is only a client available for the Metrics V3 API.

For more details see:

https://developer.conviva.com/docs/overview/b36bd03b81f71-conviva-video-ap-is-overview


Examples
========

``Examples set to use mock server, add an API Key and uncomment to use production examples``

.. code-block:: python

    from metricsv3 import MetricsV3
    
    
    ## Initilize the client.
    # metrics_client = MetricsV3("CONVIVA_API_KEY")
    metrics_client = MetricsV3()
    
    ## Get metrics using default arguements.
    # response = metrics_client.get_metric(metric="video-start-time")
    response = metrics_client.get_metric(metric="video-start-time", mock=True)
    print(response)
    
    ## Real-time metrics using default arguements.
    # response = metrics_client.get_metric(live=True, metric="video-start-time")
    response = metrics_client.get_metric(live=True, metric="video-start-time", mock=True)
    print(response)
    
    ## Group the results by a dimension.
    # response = metrics_client.get_metric(
    #     metric="rebuffering-ratio", group_by="device_name")
    response = metrics_client.get_metric(
        metric="rebuffering-ratio", group_by="device-name", mock=True)
    print(response)
    
    ## Advanced Request
    r_params = {
        "metric": "video-start-time",
        "mock": True,
        "days": 1,
        "granularity": "PT15M",
        "tag": "customTagName=Value"
    }
    response = metrics_client.get_metric(**r_params)
    print(response)
    
