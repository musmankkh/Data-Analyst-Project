# Schema Analysis Results

**Source:** `s3://movielens-elt-project/bronze_layer/`  
**Generated:** 20251220_155425  
**Total Files:** 6

## Files Generated

- `schemas_detail.json` - Detailed schema information in JSON format
- `regular_s3.sql` - Standard S3 external table DDL
- `s3_tables_ctas.sql` - S3 Tables (Iceberg) using CTAS approach
- `s3_tables_direct.sql` - Direct S3 Tables (Iceberg) creation
- `all_ddl_statements.sql` - All DDL statements combined
- `schema_summary.txt` - Human-readable schema summary

## Usage

1. Review the schema summary in `schema_summary.txt`
2. Choose the appropriate DDL from the SQL files
3. Update database names and locations as needed
4. Execute in AWS Athena

## Next Steps

- Replace `database_name` with your actual database name
- Replace `your-s3-tables-bucket` with your actual bucket for Iceberg tables
- Review and test the DDL statements before production use
