# Spectrum Shades LLC - Python Data Associate Practical Exam
# DataCamp Certification Project
# Submission 2 Results: Tasks 2, 3, 4 PASSED | Task 1 partial (data type conversion passed)

import pandas as pd

# =============================================================================
# TASK 1: Data Cleaning
# Output: clean_data DataFrame
# =============================================================================

# Load data
clean_data = pd.read_csv('production_data.csv')

# Fix raw_material_supplier: map numeric codes to string labels
# Replace non-standard missing representations first
clean_data['raw_material_supplier'] = clean_data['raw_material_supplier'].replace(
    {1: 'national_supplier', 2: 'international_supplier'}
)
# Fill remaining missing values with 'national_supplier'
clean_data['raw_material_supplier'] = clean_data['raw_material_supplier'].fillna('national_supplier')

# Fix pigment_type: valid values are type_a, type_b, type_c; anything else -> 'other'
valid_pigments = ['type_a', 'type_b', 'type_c']
clean_data['pigment_type'] = clean_data['pigment_type'].apply(
    lambda x: x if x in valid_pigments else 'other'
)

# Fix pigment_quantity: range 1-100, fill missing with median
pq_median = clean_data['pigment_quantity'].median()
clean_data['pigment_quantity'] = pd.to_numeric(clean_data['pigment_quantity'], errors='coerce')
clean_data['pigment_quantity'] = clean_data['pigment_quantity'].apply(
    lambda x: x if pd.notna(x) and 1 <= x <= 100 else float('nan')
)
clean_data['pigment_quantity'] = clean_data['pigment_quantity'].fillna(pq_median)

# Fix mixing_time: fill missing with mean, rounded to 2 decimal places
mt_mean = round(clean_data['mixing_time'].mean(), 2)
clean_data['mixing_time'] = clean_data['mixing_time'].fillna(mt_mean)

# Fix mixing_speed: fill missing with 'Not Specified'
clean_data['mixing_speed'] = clean_data['mixing_speed'].fillna('Not Specified')

# Fix product_quality_score: fill missing with mean, rounded to 2 decimal places
pqs_mean = round(clean_data['product_quality_score'].mean(), 2)
clean_data['product_quality_score'] = clean_data['product_quality_score'].fillna(pqs_mean)

# Convert production_date to datetime
clean_data['production_date'] = pd.to_datetime(clean_data['production_date'])

# Ensure batch_id is integer
clean_data['batch_id'] = clean_data['batch_id'].astype(int)


# =============================================================================
# TASK 2: Aggregation
# Output: aggregated_data DataFrame
# Columns: raw_material_supplier, avg_product_quality_score, avg_pigment_quantity
# =============================================================================

df2 = pd.read_csv('production_data.csv')

# Map raw_material_supplier codes to string labels
df2['raw_material_supplier'] = df2['raw_material_supplier'].replace(
    {1: 'national_supplier', 2: 'international_supplier'}
)

# Group by raw_material_supplier and calculate averages
aggregated_data = df2.groupby('raw_material_supplier', as_index=False).agg(
    avg_product_quality_score=('product_quality_score', 'mean'),
    avg_pigment_quantity=('pigment_quantity', 'mean')
)

# Round to 2 decimal places
aggregated_data['avg_product_quality_score'] = aggregated_data['avg_product_quality_score'].round(2)
aggregated_data['avg_pigment_quantity'] = aggregated_data['avg_pigment_quantity'].round(2)


# =============================================================================
# TASK 3: Conditional Extraction
# Output: pigment_data DataFrame (1 row)
# Filter: raw_material_supplier == 2 (international) AND pigment_quantity > 35
# Columns: raw_material_supplier, pigment_quantity, avg_product_quality_score
# =============================================================================

df3 = pd.read_csv('production_data.csv')

# Filter: raw_material_supplier == 2 AND pigment_quantity > 35
filtered = df3[(df3['raw_material_supplier'] == 2) & (df3['pigment_quantity'] > 35)]

# Build 1-row DataFrame grouped by raw_material_supplier
pigment_data = filtered.groupby('raw_material_supplier', as_index=False).agg(
    pigment_quantity=('pigment_quantity', 'mean'),
    avg_product_quality_score=('product_quality_score', 'mean')
)

# Round to 2 decimal places
pigment_data['pigment_quantity'] = pigment_data['pigment_quantity'].round(2)
pigment_data['avg_product_quality_score'] = pigment_data['avg_product_quality_score'].round(2)

# Map supplier code to string label
pigment_data['raw_material_supplier'] = pigment_data['raw_material_supplier'].replace(
    {1: 'national_supplier', 2: 'international_supplier'}
)


# =============================================================================
# TASK 4: Measures of Center, Spread, and Correlation
# Output: product_quality DataFrame (1 row)
# Columns: product_quality_score_mean, product_quality_score_sd,
#          pigment_quantity_mean, pigment_quantity_sd, corr_coef
# =============================================================================

df4 = pd.read_csv('production_data.csv')

# Calculate mean and standard deviation
pqs_mean = round(df4['product_quality_score'].mean(), 2)
pqs_sd = round(df4['product_quality_score'].std(), 2)
pq_mean = round(df4['pigment_quantity'].mean(), 2)
pq_sd = round(df4['pigment_quantity'].std(), 2)

# Pearson correlation coefficient between pigment_quantity and product_quality_score
corr = round(df4['pigment_quantity'].corr(df4['product_quality_score']), 2)

# Build product_quality DataFrame
product_quality = pd.DataFrame({
    'product_quality_score_mean': [pqs_mean],
    'product_quality_score_sd': [pqs_sd],
    'pigment_quantity_mean': [pq_mean],
    'pigment_quantity_sd': [pq_sd],
    'corr_coef': [corr]
})
