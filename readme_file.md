# Dashboard Column Analyzer

A tool that analyzes Databricks AI/BI dashboards, and surfaces only the columns which are actually utilised in your dashboards, improving pipeline & dashboard performance.

## 📋 Table of Contents

- [What Does It Do?](#what-does-it-do)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Usage Examples](#usage-examples)
- [Output Files](#output-files)
- [Step-by-Step Workflow](#step-by-step-workflow)
- [Troubleshooting](#troubleshooting)

## 🎯 What Does It Do?

This tool helps you optimize your data pipelines by:

1. **Extracting** all columns used in your Databricks dashboard
2. **Comparing** them against your actual table schemas
3. **Identifying** unused columns that can be removed from ETL pipelines
4. **Generating** ready-to-copy SQL statements with only the needed columns

**Why is this useful?**
- Reduces compute costs by processing fewer columns
- Speeds up query execution
- Makes pipelines easier to maintain
- Reduces storage requirements

## ✅ Prerequisites

- Python 3.7 or higher
- Access to your Databricks dashboard export (JSON file)
- Access to Databricks SQL or notebooks to run the generated queries (optional)

## 📦 Installation

1. **Clone this repo:**
   ```bash
   # files required
   - extract_expressions.py
   - column_lens.py
   ```

2. **No additional packages needed!** The tool uses only Python standard library.

## 🚀 Quick Start

### Step 1: Export Your Dashboard

1. Open your Databricks dashboard
2. Click the **kebab menu** (⋮) in the top right
3. Select **"Export"**
4. Save the JSON file (e.g., `my_dashboard.json`)

### Step 2: Run the Tool

```bash
python column_lens.py my_dashboard.json
```

That's it! The tool will generate all the necessary files in the `./output` directory.

## 📚 Usage Examples

### Basic Usage (SQL Output)

```bash
python column_lens.py my_dashboard.json
```

### Generate Python/PySpark Code

```bash
python column_lens.py my_dashboard.json --format python
```

### Generate Both SQL and Python

```bash
python column_lens.py my_dashboard.json --format both
```

### Custom Output Directory

```bash
python column_lens.py my_dashboard.json --output ./my_analysis
```

### Print to Console Only (No Files)

```bash
python column_lens.py my_dashboard.json --no-save
```

### Get Help

```bash
python column_lens.py --help
```

## 📂 Output Files

After running the tool, you'll find these files in the `./output` directory:

### `00_COLUMNS_TO_COPY.txt` ⭐ **START HERE**

Ready-to-paste column lists in 5 different formats for each table:

```text
-- Format 1: Simple comma-separated
order_id, customer_id, order_date, amount

-- Format 2: With backticks
`order_id`, `customer_id`, `order_date`, `amount`

-- Format 3: Formatted (one per line)
`order_id`,
    `customer_id`,
    `order_date`,
    `amount`

-- Format 4: Complete SELECT statement
SELECT
    `order_id`,
    `customer_id`,
    `order_date`,
    `amount`
FROM catalog.schema.orders;

-- Format 5: Python list for PySpark
columns = ["order_id", "customer_id", "order_date", "amount"]
df.select(columns)
```

### `01_table_columns.sql`

Query to fetch all columns from system tables for the tables used in your dashboard.

**Run this in Databricks SQL to see all available columns.**

### `02_lineage.sql`

Query to trace upstream dependencies using `system.access.table_lineage`.

**Shows which ETL pipelines feed into your dashboard tables.**

### `03_unused_columns.sql`

Query to identify columns that exist in tables but are NOT used in the dashboard.

**These are candidates for removal from your ETL pipelines.**

### `04_comparison_analysis.sql`

Comprehensive side-by-side comparison of all columns marked as USED or UNUSED.

**Best for getting a complete overview.**

## 🔄 Step-by-Step Workflow

### Step 1: Export Dashboard
Export your dashboard from Databricks as a JSON file.

### Step 2: Run Analysis
```bash
python column_lens.py my_dashboard.json
```

### Step 3: Review Column Usage
Open `./output/00_COLUMNS_TO_COPY.txt` to see which columns are actually used.

### Step 4: Check for Unused Columns
Run `./output/03_unused_columns.sql` in Databricks SQL to find unused columns:

```sql
-- Example result:
table_name                          | column_name        | data_type
------------------------------------|--------------------|-----------
catalog.schema.orders               | internal_notes     | string
catalog.schema.orders               | created_by_system  | string
catalog.schema.customers            | legacy_id          | bigint
```

### Step 5: Trace Upstream Pipelines
Run `./output/02_lineage.sql` to find which ETL jobs create these tables:

```sql
-- Example result:
dashboard_table              | upstream_table                    | source_type
-----------------------------|-----------------------------------|-------------
catalog.schema.orders        | catalog.bronze.raw_orders         | TABLE
catalog.schema.orders        | catalog.bronze.order_details      | TABLE
```

### Step 6: Update Your ETL Pipelines
Find the ETL jobs that create the dashboard tables and update their SELECT statements.

**Before (selecting all columns):**
```sql
SELECT * FROM catalog.bronze.raw_orders
```

**After (selecting only needed columns):**
```sql
-- Copy from 00_COLUMNS_TO_COPY.txt
SELECT
    `order_id`,
    `customer_id`,
    `order_date`,
    `amount`
FROM catalog.bronze.raw_orders
```

### Step 7: Test and Deploy
1. Test the updated pipeline in a development environment
2. Verify the dashboard still works correctly
3. Deploy to production

## 🐛 Troubleshooting

### Error: "File not found"
```bash
Error: File not found: dashboard_export.json
```
**Solution:** Check the file path. Use `ls` or `dir` to verify the file exists.

### Error: "Invalid JSON"
```bash
Error: Invalid JSON in dashboard file
```
**Solution:** Make sure you exported the dashboard correctly from Databricks. Re-export if needed.

### Error: "ModuleNotFoundError: No module named 'extract_expressions'"
```bash
ModuleNotFoundError: No module named 'extract_expressions'
```
**Solution:** Make sure both `extract_expressions.py` and `column_lens.py` are in the same directory.

### No columns extracted
```bash
Tables analyzed: 0
Unique columns used: 0
```
**Solution:** 
- Verify your dashboard uses dataset queries (not raw SQL)
- Check that the dashboard JSON contains a `datasets` array
- Make sure the dashboard has widgets with data visualizations

### Columns with special characters not recognized
If column names contain spaces or special characters, they should be wrapped in backticks in the original queries. The tool will preserve this formatting.

## 💡 Tips and Best Practices

1. **Start with one dashboard** - Analyze one dashboard at a time to avoid overwhelming changes

2. **Check dashboard functionality** - After updating ETL pipelines, verify the dashboard still displays correctly

3. **Keep a backup** - Save your original pipeline code before making changes

4. **Use Format 4** from `00_COLUMNS_TO_COPY.txt` for the quickest copy-paste experience

5. **Document changes** - Keep track of which columns you removed and why

6. **Coordinate with team** - Make sure others aren't relying on the "unused" columns

7. **Monitor query performance** - Track execution time before and after optimization

## 📊 Example Scenario

**Before Optimization:**
```sql
-- ETL Pipeline selects all 50 columns
SELECT * FROM bronze.customer_events
```

**After Running Tool:**
- Dashboard only uses 12 columns
- `03_unused_columns.sql` shows 38 unused columns
- Copy optimized SELECT from `00_COLUMNS_TO_COPY.txt`

**After Optimization:**
```sql
-- ETL Pipeline selects only 12 needed columns
SELECT
    `customer_id`,
    `event_date`,
    `event_type`,
    `revenue`,
    -- ... 8 more columns
FROM bronze.customer_events
```

**Result:** 76% reduction in data processing, faster queries, lower costs! 🎉

## 🤝 Contributing

Found a bug or have a feature request? Please create an issue or submit a pull request.

## 📝 License

This tool is provided as-is for optimizing Databricks dashboards and data pipelines.

---

**Questions?** Check the troubleshooting section or run with `--help` flag for more options.