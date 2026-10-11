"""
build_dashboard.py - writes the Media/OTT final-project dashboard as one
importable .charts file (Atlas Charts export format, exportVersion 10).

    python tools/build_dashboard.py

Same export format as the earlier OTT_Media_Vertical_Dashboard.charts, with
the fixes from the validation review:
  - completion % now measured on FINISHED sessions only (complete/abandon)
  - per-minute charts limited to the last 30 minutes (otherwise they grow to
    hundreds of bars after a few hours)
  - "last 15 minutes" operational KPIs, so an incident is visible instead of
    being diluted by hours of healthy history
  - new cross-source tiles: incident heatmap, region small-multiples,
    revenue at risk, pipeline lag, plays on expired licences, sentiment
Only chart types already used in the earlier file are used here
(Number, Donut, Grouped/Stacked Bar, Grouped/Stacked Column, Heatmap).
"""
import json
import os
import uuid

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "dashboards")
DB = "ott_streaming"
COLLECTION = "all_events"
DEPLOYMENT = "Cluster0"
DS = "src"
PALETTE = ["#1392AB", "#16CC62", "#196EE6", "#E6B219", "#E6196E", "#19C3E6", "#116149", "#E65D19",
           "#ABABAB", "#80340E", "#73E0A1", "#8CB6F2"]
RED, AMBER = "rgb(193, 4, 34)", "rgb(230, 157, 25)"


# ---------------------------------------------------------------- calculated fields
# Each is (name, expression). The expression is plain MQL aggregation syntax,
# evaluated by Charts per document. $$NOW makes the "last N minutes" windows.
def since(minutes):
    return {"$gte": ["$timestamp", {"$dateSubtract": {"startDate": "$$NOW", "unit": "minute", "amount": minutes}}]}


CALC = {
    "events_15m": {"$cond": [since(15), 1, 0]},
    "lag_sec": {"$divide": [{"$subtract": ["$ingested_at", "$timestamp"]}, 1000]},
    "buffering_15m": {"$cond": [{"$and": [since(15), {"$eq": ["$source", "cdn"]}]}, "$buffering_rate_pct", None]},
    "revenue_risk_15m": {"$cond": [{"$and": [since(15), {"$in": ["$subscription_action",
                                   ["cancellation", "payment_failed"]]}]}, "$plan_price_inr", 0]},
    "negative_pct_15m": {"$cond": [{"$and": [since(15), {"$eq": ["$source", "social"]}]},
                                   {"$cond": [{"$eq": ["$sentiment", "negative"]}, 100, 0]}, None]},
    "abandon_pct_15m": {"$cond": [{"$and": [since(15), {"$in": ["$event_type", ["abandon", "complete"]]}]},
                                  {"$cond": [{"$eq": ["$event_type", "abandon"]}, 100, 0]}, None]},
    "cancellations_15m": {"$cond": [{"$and": [since(15), {"$eq": ["$subscription_action", "cancellation"]}]}, 1, 0]},
    "minute_ist_30m": {"$cond": [since(30), {"$dateToString": {"format": "%H:%M", "date": "$timestamp",
                                                               "timezone": "Asia/Kolkata"}}, None]},
}


def calc(name):
    expr = CALC[name]
    return {"fieldPath": name, "rawExpression": json.dumps(expr, indent=2),
            "derivedMQL": json.dumps(expr, separators=(",", ":"))}


# ---------------------------------------------------------------- encodings
def cat(field, limit=None):
    c = {"field": field, "type": "nominal", "inferredType": "String", "channelType": "category"}
    if limit:
        c.update({"isLimiting": True, "limitingSize": str(limit)})
    return c


def val(field="_id", agg="count"):
    return {"field": field, "type": "quantitative",
            "inferredType": "String" if field == "_id" else "Number",
            "channelType": "aggregation", "aggregate": agg}


def lbl(text, number=False, decimals=None):
    d = {"labelOverride": {"enabled": True, "value": text}}
    if number:
        if decimals is not None:
            d["numberFormatting"] = {"enabled": True, "value": "Custom"}
            d["numberDecimals"] = {"enabled": True, "value": str(decimals)}
        else:
            d["numberFormatting"] = {"enabled": True, "value": "Default"}
    return d


def only(field, values):
    return {"fieldPath": field, "disabled": False, "type": "String",
            "settings": {"allOthers": False, "values": values}}


def exclude(field, values):
    return {"fieldPath": field, "disabled": False, "type": "String",
            "settings": {"allOthers": True, "values": values}}


def calcs_used(channels, filters):
    names = {c["field"] for c in channels.values()} | {f["fieldPath"] for f in filters or []}
    return [calc(n) for n in CALC if n in names]


def item(title, chart_type, icon, channels, cust_channels, filters=None, options=None, axes=None, cond=None):
    return {
        "title": title, "description": "", "dashboardId": None, "dataSourceId": DS,
        "iconValue": icon, "itemType": "chart", "filters": filters or [], "missedFields": [],
        "lookupFields": [], "convertedFields": [], "calculatedFields": calcs_used(channels, filters),
        "channels": channels, "reductions": {},
        "customisations": {"options": options or {}, "axes": axes or {}, "channels": cust_channels,
                           "conditionalFormatting": cond or []},
        "chartType": chart_type, "meta": {}, "sample": False, "query": None, "queryId": None,
        "interactiveFiltering": "highlight", "embedding": {"id": str(uuid.uuid4())},
    }


def color_rule(rgb, op="NUMBER_GREATER_THAN", value=0):
    return [{"type": "discrete",
             "conditions": [{"target": {"value": "value", "type": "CHANNEL"}, "operator": op, "value": str(value)}],
             "applyToEntireRow": False,
             "styling": {"color": rgb, "backgroundColor": "rgba(0, 0, 0, 0)", "fontStyle": "normal",
                         "fontWeight": "bold", "textDecoration": "initial"}}]


def number(title, field="_id", agg="count", filters=None, decimals=None, cond=None):
    fmt = ({"numberFormatting": {"enabled": True, "value": "Default"}} if decimals is None else
           {"numberFormatting": {"enabled": True, "value": "Custom"},
            "numberDecimals": {"enabled": True, "value": str(decimals)}})
    return item(title, "Number", "number", {"value": val(field, agg)}, {"value": fmt},
                filters, axes={"value": {}}, cond=cond)


def donut(title, field, label, filters=None, limit=10):
    ch = cat(field, limit)
    ch["allOthers"] = True
    return item(title, "Donut", "donut", {"label": ch, "value": val()},
                {"label": lbl(label), "value": {"numberFormatting": {"enabled": True, "value": "Default"}}},
                filters, axes={"value": {}})


def hbar(title, field, label, vfield="_id", agg="count", vlabel="Count", color=None, color_label=None,
         filters=None, limit=10, single=None, decimals=None):
    ch = {"y": cat(field, limit), "x": val(vfield, agg)}
    cc = {"y": lbl(label), "x": lbl(vlabel, True, decimals)}
    opts = {"dataValueLabels": {"enabled": True, "value": None}}
    if color:
        ch["color"] = cat(color)
        cc["color"] = lbl(color_label or color)
        opts = {"colorDiscrete": {"enabled": True, "value": PALETTE}}
        kind, icon = "Stacked Bar", "bar-stacked"
    else:
        kind, icon = "Grouped Bar", "bar-grouped"
        if single:
            opts["colorDiscrete"] = {"enabled": True, "value": [single]}
    return item(title, kind, icon, ch, cc, filters, options=opts, axes={"x": {}})


def column(title, field, label, vfield="_id", agg="count", vlabel="Count", color=None, color_label=None,
           filters=None, single=None):
    ch = {"x": cat(field), "y": val(vfield, agg)}
    cc = {"x": lbl(label), "y": lbl(vlabel, True)}
    opts = {}
    if color:
        ch["color"] = cat(color)
        cc["color"] = lbl(color_label or color)
        opts = {"colorDiscrete": {"enabled": True, "value": PALETTE}}
        kind, icon = "Stacked Column", "column-stacked"
    else:
        kind, icon = "Grouped Column", "column-grouped"
        if single:
            opts = {"colorDiscrete": {"enabled": True, "value": [single]}}
    return item(title, kind, icon, ch, cc, filters, options=opts,
                axes={"x": {"categoryLabelAngle": {"enabled": True, "value": "diagonal"}}})


def heatmap(title, xf, xl, yf, yl, vfield="_id", agg="count", clabel="Count", filters=None, decimals=None):
    return item(title, "Heatmap", "heatmap", {"x": cat(xf), "y": cat(yf), "color": val(vfield, agg)},
                {"x": lbl(xl), "y": lbl(yl), "color": lbl(clabel, True, decimals)},
                filters, axes={"x": {"categoryLabelAngle": {"enabled": True, "value": "diagonal"}}})


# ---------------------------------------------------------------- layout
def build(fname, title, description, rows):
    items, layout, y, n = {}, [], 0, 0
    for row_h, row in rows:
        x = 0
        for chart, w in row:
            n += 1
            key = f"item-{n}"
            c = dict(chart)
            c["dashboardId"] = "dashboard-1"
            items[key] = c
            layout.append({"w": w, "h": row_h, "x": x, "y": y, "i": key})
            x += w
        assert x == 8, f"{title}: row at y={y} is {x} wide, must total 8"
        y += row_h
    out = {"exportVersion": 10,
           "dashboards": {"dashboard-1": {"title": title, "layout": layout, "description": description,
                                          "embedding": {"anonymousAuthEnabled": False}, "filters": [
               {"disabled": False, "type": "String", "settings": {"allOthers": True, "values": []},
                "name": name, "linkedFields": [{"dataSourceId": DS, "fieldPath": f}]}
               for name, f in [("Region", "region"), ("CDN node", "cdn_node"), ("Source", "source"),
                               ("Title", "title"), ("User", "user_id")]
           ]}},
           "items": items,
           "dataSources": {DS: {"alias": f"OTT: {COLLECTION}", "collection": COLLECTION, "database": DB,
                                "deployment": DEPLOYMENT, "sourceType": "cluster"}},
           "queries": {}}
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, fname), "w") as f:
        json.dump(out, f)
    print(f"{fname}: {len(items)} tiles, grid height {y}")
    return out


PLAYBACK = only("source", ["playback"])
METADATA = only("source", ["metadata"])
SUBSCRIPTION = only("source", ["subscription"])
CDN = only("source", ["cdn"])
SOCIAL = only("source", ["social"])
FINISHED = only("event_type", ["complete", "abandon"])
LAST30 = exclude("minute_ist_30m", [None])

ROWS = [
    # Row A - is the platform healthy RIGHT NOW? (last 15 minutes)
    (1, [(number("Events - last 15 min", "events_15m", "sum"), 2),
         (number("Pipeline Lag (sec)", "lag_sec", "mean", decimals=2), 2),
         (number("Avg Buffering % - last 15 min", "buffering_15m", "mean", decimals=1,
                 cond=color_rule(RED, value=5)), 2),
         (number("Revenue at Risk (INR) - last 15 min", "revenue_risk_15m", "sum",
                 cond=color_rule(AMBER, value=0)), 2)]),
    # Row B - standing risks
    (1, [(number("Plays on EXPIRED Licences", filters=[PLAYBACK, only("license_status", ["expired"])],
                 cond=color_rule(RED)), 2),
         (number("Payment Failures", filters=[only("subscription_action", ["payment_failed"])],
                 cond=color_rule(AMBER)), 2),
         (number("CDN Errors", filters=[CDN, exclude("error_code", [None])], cond=color_rule(RED)), 2),
         (number("Negative Social % - last 15 min", "negative_pct_15m", "mean", decimals=0,
                 cond=color_rule(RED, value=40)), 2)]),
    # Row 1 - WHERE is it breaking? Incident shows as a hot band on one node
    (2, [(heatmap("1. Buffering % by CDN Node per Minute (last 30 min)", "minute_ist_30m", "Minute (IST)",
                  "cdn_node", "CDN node", "buffering_rate_pct", "mean", "Avg buffering %",
                  filters=[CDN, LAST30], decimals=1), 8)]),
    # Row 2 - WHAT does it cost? Same region lights up in four different source systems
    (2, [(hbar("2. Buffering % by Region (15 min)", "region", "Region", "buffering_15m", "mean",
               "Avg buffering %", single="#E6196E", decimals=1), 2),
         (hbar("3. Abandon Rate % by Region (15 min)", "region", "Region", "abandon_pct_15m", "mean",
               "% of finished sessions abandoned", single="#E65D19", decimals=0), 2),
         (hbar("4. Negative Social % by Region (15 min)", "region", "Region", "negative_pct_15m", "mean",
               "% negative", single="#E6B219", decimals=0), 2),
         (hbar("5. Cancellations by Region (15 min)", "region", "Region", "cancellations_15m", "sum",
               "Cancellations", single="#196EE6"), 2)]),
    # Row 3 - pipeline throughput
    (2, [(column("6. Events per Minute by Source (last 30 min)", "minute_ist_30m", "Minute (IST)",
                 vlabel="# Events", color="source", color_label="Source", filters=[LAST30]), 5),
         (donut("7. Events by Source", "source", "Source"), 3)]),
    # Row 4 - engagement
    (2, [(hbar("8. Avg Watch Completion % by Genre (finished sessions)", "genre", "Genre",
               "watch_completion_pct", "mean", "Avg completion %", filters=[PLAYBACK, FINISHED],
               single="#1392AB", decimals=1), 4),
         (hbar("9. Session Outcome by Stream Quality", "session_quality", "Stream quality",
               vlabel="# Finished sessions", color="event_type", color_label="Outcome",
               filters=[PLAYBACK, FINISHED]), 4)]),
    # Row 5 - revenue
    (2, [(hbar("10. Subscription Actions", "subscription_action", "Action", vlabel="# Events",
               filters=[SUBSCRIPTION], single="#196EE6"), 4),
         (donut("11. Subscriber Plan Mix", "plan", "Plan", filters=[SUBSCRIPTION]), 4)]),
    # Row 6 - delivery infrastructure
    (2, [(hbar("12. Avg Latency by CDN Node (ms)", "cdn_node", "CDN node", "latency_ms", "mean",
               "Avg latency (ms)", filters=[CDN], single="#E65D19", decimals=0), 4),
         (donut("13. CDN Error Codes", "error_code", "Error code", filters=[CDN, exclude("error_code", [None])]), 4)]),
    # Row 7 - brand
    (2, [(hbar("14. Mentions by Title and Sentiment", "title", "Title", vlabel="# Mentions",
               color="sentiment", color_label="Sentiment", filters=[SOCIAL]), 4),
         (donut("15. Social Mentions by Platform", "platform", "Platform", filters=[SOCIAL]), 4)]),
    # Row 8 - content rights
    (2, [(hbar("16. Plays on Expired / Expiring Licences by Title", "title", "Title", vlabel="# Plays",
               color="license_status", color_label="Licence status",
               filters=[PLAYBACK, only("license_status", ["expired", "expiring_24h"])]), 4),
         (donut("17. Playback Device Mix", "device", "Device", filters=[PLAYBACK]), 4)]),
]

if __name__ == "__main__":
    build("OTT_Media_Final_Dashboard.charts", "Media / OTT - Real-Time Operations Dashboard",
          "Five sources (playback, content licensing, billing, CDN telemetry, social) streamed through "
          "Kafka into one collection, ott_streaming.all_events. Top two rows = last 15 minutes.", ROWS)
