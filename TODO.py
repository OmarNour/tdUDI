"""
TODO:
    handle "select *", in pipelines
    generate fake data
    USE CONFIG FILE, one for server and another for client
    handle unified data
    DONE Populate lookup core tables, by creating BAMP TXF
    DONE Validate columns' names for lookup core tables, are <table_name>_CD, <table_name>_DESC
    DONE finish generate_metadata_scripts
    generate grant access, script., examnple "GRANT SELECT ON gdev1t_srci TO gdev1v_inp WITH GRANT OPTION;"
    DONE add technical columns for all tables (src, stg, srci & core), if not exists in the SMX
    DONE SRCI view, to cast the data type when joining with the utlfw table
    DONE Class to hold error data, then print it in a  proper way for end user!
    return all data from all classes in DF
    DataSet:
        DONE set_name, should be changed tp core_table_id
    Create new objects, one for attribute names only, and the other for entity names only, then the both will create tables and columns!
    Rejection tables and rules, to be distinguished
    Connect to the DB, to deploy and report issues
    Data Lineage report
    Data Quality report
    DataBase:
        DONE reserved_words list
        DONE trx validation
    Layer:
        DONE add new attribute, to distinguish between different layers: 0 landing, 1 staging, 2 surrogate, 3, srci, 4 core model
        DONE create new class, named LayerType
    ColumnMapping:
        DONE raise error if trx is invalid for columns mapping
    Pipeline:
        DONE A function to get table by alias
        DONE Handle alias for main tables
        handle alias in difference expressions (col, where, etc.)
        Handle alias for join tables
        DONE use ALPHABETS.pop(), to get aliases!
        DONE handle transactional_data flag, to differentiate between trans and non-trans data
        DONE change Pipeline.table_id, to Pipeline.layer_view_id
        DONE change the key to be, src_lyr_table_id, tgt_lyr_table_id, layer_view_id
    GroupBy:
        To handle agg functions
    Filter & OrFilter:
        Comparison operators:
            "="             (equal to)
            "<>" or "!="    (not equal to)
            ">"             (greater than)
            ">="            (greater than or equal to)
            "<"             (less than)
            "<="            (less than or equal to)
            "BETWEEN"       (between a range of values)
            "IN"            (matches any value in a set)
            "LIKE"          (matches a pattern)
            "IS NULL"       (checks for null values)
            "IS NOT NULL"   (checks for non-null values)
        Logical operators:
            "AND"           (logical and)
            "OR"            (logical or)
            "NOT"           (logical not)
        Set operators:
            "UNION"         (combines the results of two or more SELECT statements)
            "INTERSECT"     (returns only the rows that are common to two SELECT statements)
            "EXCEPT"        (returns only the rows that are unique to the first SELECT statement)
            "MINUS"         (returns only the rows that are unique to the first SELECT statement)
        Arithmetic operators:
            "+"             (addition)
            "-"             (subtraction)
            "*"             (multiplication)
            "/"             (division)
        Aggregate functions:
            "SUM"           (returns the sum of all values in a column)
            "AVG"           (returns the average of all values in a column)
            "COUNT"         (returns the number of rows in a column)
            "MAX"           (returns the maximum value in a column)
            "MIN"           (returns the minimum value in a column)
"""
