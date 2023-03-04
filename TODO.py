# TODO:
#   DataSet:
#       set_name, should be changed tp core_table_id
#   change Pipeline.table_id, to Pipeline.view_id
#   Create new objects, one for attribute names only, and the other for entity names only, then the both will create tables and columns!
#   Rejection tables and rules, to be distinguished
#   Connect to the DB, to deploy and report issues
#   Data Lineage report
#   Data Quality report
#   DataBase:
#       DONE reserved_words list
#       DONE trx validation
#   Layer:
#       add new attribute, to distinguish between different layers: 0 landing, 1 staging, 2 surrogate, 3, srci, 4 core model
#       create new class, named LayerType
#   ColumnMapping:
#       DONE raise error if trx is invalid for columns mapping
#   Pipeline:
#       DONE Handle alias for main tables
#       handle alias in difference expressions (col, where, etc.)
#       Handle alias for join tables
#       use ALPHABETS.pop(), to get aliases!
#       handle transactional_data flag, to differentiate between trans and non-trans data
#   GroupBy:
#       To handle agg functions
#   Filter & OrFilter:
#       Comparison operators:
#           "="             (equal to)
#           "<>" or "!="    (not equal to)
#           ">"             (greater than)
#           ">="            (greater than or equal to)
#           "<"             (less than)
#           "<="            (less than or equal to)
#           "BETWEEN"       (between a range of values)
#           "IN"            (matches any value in a set)
#           "LIKE"          (matches a pattern)
#           "IS NULL"       (checks for null values)
#           "IS NOT NULL"   (checks for non-null values)
#        Logical operators:
#           "AND"           (logical and)
#           "OR"            (logical or)
#           "NOT"           (logical not)
#        Set operators:
#           "UNION"         (combines the results of two or more SELECT statements)
#           "INTERSECT"     (returns only the rows that are common to two SELECT statements)
#           "EXCEPT"        (returns only the rows that are unique to the first SELECT statement)
#           "MINUS"         (returns only the rows that are unique to the first SELECT statement)
#        Arithmetic operators:
#           "+"             (addition)
#           "-"             (subtraction)
#           "*"             (multiplication)
#           "/"             (division)
#        Aggregate functions:
#           "SUM"           (returns the sum of all values in a column)
#           "AVG"           (returns the average of all values in a column)
#           "COUNT"         (returns the number of rows in a column)
#           "MAX"           (returns the maximum value in a column)
#           "MIN"           (returns the minimum value in a column)

