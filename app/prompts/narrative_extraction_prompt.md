You are extracting structured crude oil market narrative events from a document chunk.

Return an object matching the NarrativeExtraction schema.

Extraction rules:
1. Only extract if the text is directly relevant to crude oil pricing, supply, demand, inventories, OPEC policy, sanctions, shipping, refining margins, macro oil demand, or geopolitics affecting oil.
2. If the chunk is irrelevant, repetitive boilerplate, or too weak to support an event, set should_extract=false.
3. Use these preferred topics when possible:
   - supply_disruption
   - demand_reacceleration
   - inventory_draw
   - inventory_build
   - opec_policy
   - sanctions_risk
   - weather_risk
   - shipping_disruption
   - refining_margin_shift
   - macro_growth_fear
   - usd_rates_pressure
   - geopolitical_risk
   - production_restart
   - other
4. direction must be one of: bullish, bearish, mixed, neutral.
5. verification_status must be one of: officially_confirmed, partially_confirmed, unverified, refuted.
6. horizon must be one of: intraday, swing, medium_term.
7. rumor_flag=true for chatter, hearsay, transmission, or unverified forwarding.
8. evidence_text should be a short exact excerpt or close excerpt from the chunk supporting the extraction.
9. Use conservative credibility and confidence values. Do not overstate certainty.
