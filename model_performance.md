## Model Performance (Weekly Backtest)

This project includes a **weekly self-validation (backtest)** step to show whether the demand forecast model is performing better than a simple baseline, and to prevent low-quality forecasts from driving restock recommendations.

### What “Backtest” Means (Simple Explanation)

Every week, I:
1. **Train** a model on historical weekly sales **excluding the most recent 4 completed weeks**.
2. **Predict (forecast)** those last **4 completed weeks** (the “holdout window”).
3. Compare the model’s predictions to the **actual sales** during the holdout window.
4. Compare model performance to a **baseline** forecast: “previous week’s sales for the same item.”

This provides a repeatable, automated way to validate forecast quality.

### Key Metric: WAPE

I use **WAPE (Weighted Absolute Percentage Error)** per `variant_id`:

- `WAPE = SUM(|actual - predicted|) / SUM(actual)`

WAPE is preferred over MAPE for retail data because it is more stable with low volume items and avoids over-weighting tiny denominators.

### Baseline Comparison

I compute a simple baseline WAPE for each `variant_id`:
- **Baseline forecast** = sales from the **previous week** for the same item

I label items as “WORSE_THAN_BASELINE” when the model performs worse than this baseline on the holdout window.

### Model Quality Flags (Used in Restock Gating)

The backtest produces `model_quality_flags` with a `model_quality` label per `variant_id`:
- `GOOD` — model meets the performance threshold (and/or beats baseline)
- `WEAK` / `BAD` — model error is too large
- `WORSE_THAN_BASELINE` — model is worse than the simple baseline
- `NO_DATA` — not enough demand signal in the holdout window (common in long-tail retail)

**Important note:** Many retail catalogs have long-tail items with sparse sales. A high “NO_DATA” rate often reflects low recent sales volume (not model failure). In the dashboard, I focus model-performance visuals on **scorable** items (e.g., holdout volume above a threshold).

### How Performance Affects the Restock Output

The weekly restock table uses a **quality gate**:
- If `model_quality = GOOD`, I use forecast demand (ML).
- Otherwise, I use a robust fallback estimate:
  - **Fallback:** average daily units over the last 56 days × the restock horizon window.

This keeps restock recommendations stable and defensible even when the model has low confidence.

### Tables Used for Model Proof

The weekly model validation produces:
- `backtest_forecast_4w`: predicted weekly quantities for the holdout window
- `backtest_metrics_variant_4w`: WAPE/MAPE per item for the holdout window
- `backtest_baseline_4w`: baseline WAPE per item
- `model_quality_flags`: final quality label per item
- `backtest_proof_4w`: weekly table with **actual vs predicted vs baseline** (used by the dashboard proof charts)

### What Success Looks Like

A successful iteration of this system shows:
- Forecast accuracy improves as item sales volume increases (high-volume variants are most predictable).
- For scorable items, a meaningful share of variants are `GOOD` and/or beat the baseline.
- Restock outputs remain stable week-to-week and align with business constraints (lead times, pack sizes, MOQs).
- Negative stock and mapping anomalies are surfaced clearly and do not silently corrupt reorder math.

### Key Takeaways

- I use `variant_id` as the canonical key; SKUs are manually created and can collide across vendors/items.
- Sparse retail catalogs require fallbacks; many items do not have enough weekly signal to score reliably.
- Baseline comparisons help avoid overclaiming model quality.
- Quality gates prevent low-quality forecasts from driving operational decisions.

---

## Next Steps

### 1) Improve Coverage & Accuracy
- Experiment with different granularities (daily vs weekly) and compare performance on high-volume items.
- Add seasonality features or product groupings (if category data becomes available).
- Evaluate alternative approaches for sparse series (e.g., pooled/hierarchical methods) and compare to baseline.

### 2) Discontinue / Slow-Mover Report
Add a separate report for items that:
- have **no sales in the last 90 days**, and
- still have inventory on hand,
to support pruning decisions and prevent unnecessary restocks.

### 3) Vendor & Item Archiving Workflow (Write-back)
Looker Studio is read-only. To let stakeholders mark vendors/items as “Archived,” I can:
- maintain `vendor_status` (and an optional `variant_status`) via a lightweight Google Sheet or form,
- ingest those flags into BigQuery, and
- filter archived vendors/items out of restock generation automatically.

### 4) Operational Hardening
- Add pipeline health checks/alerts (missing snapshots, missing sales loads, spikes in negative stock).
- Keep BigQuery spend predictable with budgets + alerts and enforce partition filters on raw tables.

### 5) Dashboard Enhancements
- Continue refining the Model Performance page (coverage threshold controls, scatter vs baseline, and per-variant proof time series).
- Add vendor-level rollups (e.g., `vendor_stock_summary_weekly`) for faster vendor summary KPIs.