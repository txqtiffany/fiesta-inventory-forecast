## Model Performance (Weekly Backtest)

This project includes a **weekly self-validation (backtest)** step to demonstrate whether the demand forecast model is performing better than a simple baseline, and to prevent low-quality forecasts from driving restock recommendations.

### What “Backtest” Means (Simple Explanation)

Every week, we:
1. **Train** a model on historical weekly sales **excluding the most recent 4 completed weeks**.
2. **Predict (forecast)** those last **4 completed weeks** (the “holdout window”).
3. Compare the model’s predictions to the **actual sales** during the holdout window.
4. Compare model performance to a **baseline** forecast: “previous week’s sales for the same item.”

This provides a repeatable, automated way to prove the model is behaving well and to detect when it is not.

### Key Metric: WAPE

We use **WAPE (Weighted Absolute Percentage Error)** per `variant_id`:

- `WAPE = SUM(|actual - predicted|) / SUM(actual)`

WAPE is preferred over MAPE for retail data because it is more stable with low volume items and avoids over-weighting tiny denominators.

### Baseline Comparison

We compute a simple baseline WAPE for each `variant_id`:
- **Baseline forecast** = sales from the **previous week** for the same item

We label items as “WORSE_THAN_BASELINE” when the model performs worse than this baseline on the holdout window.

### Model Quality Flags (Used in Restock Gating)

The backtest produces `model_quality_flags` with a `model_quality` label per `variant_id`:
- `GOOD` — model meets performance threshold (and/or beats baseline)
- `WEAK` / `BAD` — model error is too large
- `WORSE_THAN_BASELINE` — model is worse than the simple baseline
- `NO_DATA` — not enough demand signal in the holdout window (very common in long-tail retail)

**Important note:** Many retail catalogs have long-tail items with sparse sales. A high “NO_DATA” rate usually reflects low recent sales volume (not model failure). To avoid misleading metrics, dashboard views focus on **scorable** items (e.g., holdout volume above a threshold).

### How Performance Affects the Restock Output

The weekly restock table uses a **quality gate**:
- If `model_quality = GOOD`, use forecast demand (ML)
- Otherwise, use a robust fallback estimate:
  - **Fallback:** average daily units over the last 56 days × restock horizon window

This ensures that restock recommendations remain stable and defensible even when the model has low confidence.

### Outputs Used for “Proof” in the Dashboard

The weekly model validation generates:
- `backtest_forecast_4w`: predicted weekly quantities for the holdout window
- `backtest_metrics_variant_4w`: WAPE/MAPE per item for the holdout window
- `backtest_baseline_4w`: baseline WAPE per item
- `model_quality_flags`: final quality label per item
- `backtest_proof_4w`: weekly table with **actual vs predicted vs baseline** (best for demonstration charts)

### What Success Looks Like

A successful iteration of this system shows:
- Model WAPE improves as item volume increases (high-volume variants are most predictable).
- For scorable items, a meaningful share of items are `GOOD` and/or beat baseline.
- Restock outputs are stable week-to-week and align with business expectations (lead times, pack sizes, MOQs).
- Negative stock and mapping anomalies are surfaced clearly and do not silently corrupt reorder math.

### Practical Takeaways / Lessons Learned

- **Use `variant_id` as the canonical key.** SKUs are manually created and may collide across vendors/items.
- **Forecasting sparse retail catalogs requires fallbacks.** Most items do not have enough weekly signal to score reliably.
- **Baseline comparisons matter.** A simple baseline often performs surprisingly well; comparing to it avoids overclaiming.
- **Quality gates prevent bad ML from becoming bad operations.** This is critical for business trust.

---

## Next Steps

### 1) Improve Coverage & Accuracy
- Experiment with a different granularity:
  - Weekly vs daily (current is weekly; daily may help for high-volume items, but increases sparsity noise)
- Add seasonality features or product category grouping (if available).
- Try alternative models for sparse series (e.g., pooled models, hierarchical forecasting) and compare to baseline.

### 2) Discontinue / Slow-Mover Report
Add a separate report for items that:
- have **no sales in the last 90 days** AND
- still have inventory on hand
This supports pruning decisions and prevents unnecessary restocks.

### 3) Vendor & Item Archiving Workflow (Write-back)
Looker Studio is read-only. To allow stakeholders to mark vendors/items as “Archived”:
- Use a lightweight Google Sheet (or form) as the source of truth for:
  - `vendor_status.archived`
  - optional `variant_status.archived`
- Ingest that into BigQuery and apply it as a filter in restock generation.

### 4) Operational Hardening
- Add alerting on pipeline health:
  - missing inventory snapshot
  - missing sales loads
  - sudden spikes in negative stock counts
- Add BigQuery budgets/alerts and ensure partition filters are enforced on all raw tables.

### 5) Dashboard Enhancements
- Add a “Model Performance” page with:
  - quality distribution
  - scatter: baseline_wape vs model_wape
  - time series: actual vs predicted for selected `variant_id`
- Add vendor-level rollups (`vendor_stock_summary_weekly`) for fast, clear vendor summary KPIs.