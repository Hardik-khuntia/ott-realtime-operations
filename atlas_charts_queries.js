// =====================================================================
// ONE TILE TO ADD BY HAND (2 minutes) - everything else is in
// dashboards/OTT_Media_Final_Dashboard.charts
//
// "18. Buffering Experienced: Abandoned vs Completed Sessions"
// Joins CDN pings to playback sessions on session_id - the strongest
// cross-source evidence that buffering causes abandonment.
//
// How: open the dashboard -> Add Chart -> data source OTT: all_events
//   -> chart type Bar > Grouped Column -> paste the pipeline below into
//   the Query bar -> Apply
//   -> X axis: outcome   Y axis: avg_buffering_pct (aggregate: Mean)
//   -> drag `sessions` into the Label/Tooltip if available -> Save
//
// If the Query bar rejects $$NOW / $dateSubtract, delete the $expr line
// (it then uses all data in the collection, which the 12h TTL already caps).
// =====================================================================
[
  { $match: {
      source: { $in: ["cdn", "playback"] },
      session_id: { $ne: null },
      $expr: { $gte: ["$timestamp", { $dateSubtract: { startDate: "$$NOW", unit: "minute", amount: 60 } }] }
  } },
  { $group: {
      _id: "$session_id",
      avg_buffering: { $avg: "$buffering_rate_pct" },
      abandoned: { $max: { $cond: [{ $eq: ["$event_type", "abandon"] }, 1, 0] } },
      completed: { $max: { $cond: [{ $eq: ["$event_type", "complete"] }, 1, 0] } }
  } },
  { $match: { avg_buffering: { $ne: null }, $or: [{ abandoned: 1 }, { completed: 1 }] } },
  { $group: {
      _id: { $cond: [{ $eq: ["$abandoned", 1] }, "Abandoned", "Completed"] },
      sessions: { $sum: 1 },
      avg_buffering_pct: { $avg: "$avg_buffering" }
  } },
  { $project: { _id: 0, outcome: "$_id", sessions: 1, avg_buffering_pct: { $round: ["$avg_buffering_pct", 1] } } }
]
