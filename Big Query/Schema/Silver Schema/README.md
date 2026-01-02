# Schema Analysis Results

**Source:** `s3://movielens-elt-project/silver/`  
**Generated:** 20251221_132625  
**Total Tables:** 6

## Folder Structure

Each folder represents a table and contains:
- `regular_s3.sql` - Standard S3 external table DDL
- `s3_tables_ctas.sql` - S3 Tables (Iceberg) using CTAS approach
- `s3_tables_direct.sql` - Direct S3 Tables (Iceberg) creation
- `all_ddl_statements.sql` - All DDL statements combined

## Tables Found

- **silver_genome_scores** - `silver/silver_genome_scores/`
- **silver_genome_tags** - `silver/silver_genome_tags/`
- **silver_links** - `silver/silver_links/`
- **silver_movies** - `silver/silver_movies/`
- **silver_ratings** - `silver/silver_ratings/`
- **silver_tags** - `silver/silver_tags/`

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
