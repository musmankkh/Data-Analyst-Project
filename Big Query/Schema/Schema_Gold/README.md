# Schema Analysis Results

**Source:** `s3://movielens-elt-project/gold/`  
**Generated:** 20251221_140710  
**Total Tables:** 14

## Folder Structure

Each folder represents a table and contains:
- `regular_s3.sql` - Standard S3 external table DDL
- `s3_tables_ctas.sql` - S3 Tables (Iceberg) using CTAS approach
- `s3_tables_direct.sql` - Direct S3 Tables (Iceberg) creation
- `all_ddl_statements.sql` - All DDL statements combined

## Tables Found

- **dim_date** - `gold/dim_date/`
- **dim_genres** - `gold/dim_genres/`
- **dim_movies** - `gold/dim_movies/`
- **dim_tags** - `gold/dim_tags/`
- **dim_users** - `gold/dim_users/`
- **fact_genome_scores** - `gold/fact_genome_scores/`
- **fact_user_tags** - `gold/fact_user_tags/`
- **kpi_executive_summary** - `gold/kpi_executive_summary/`
- **kpi_genre_combinations** - `gold/kpi_genre_combinations/`
- **kpi_genre_performance** - `gold/kpi_genre_performance/`
- **kpi_movie_performance** - `gold/kpi_movie_performance/`
- **kpi_tag_analytics** - `gold/kpi_tag_analytics/`
- **kpi_time_series** - `gold/kpi_time_series/`
- **kpi_user_engagement** - `gold/kpi_user_engagement/`

## Usage

1. Review `MASTER_SUMMARY.txt` for all table schemas
2. Navigate to each table's folder
3. Choose the appropriate DDL file
4. Update database names and locations as needed
5. Execute in AWS Athena

## Next Steps

- Replace `database_name` with your actual database name
- Replace `your-s3-tables-bucket` with your actual bucket for Iceberg tables
- Review and test the DDL statements before production use
