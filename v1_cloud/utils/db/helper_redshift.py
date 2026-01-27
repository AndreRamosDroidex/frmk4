# utils/db/helper_redshift.py
from typing import List, Tuple, Optional
from pyspark.sql import SparkSession

def get_rs_table_columns_order(
    spark: SparkSession,
    url: str,
    jprops: dict,
    table: str,
    default_schema: Optional[str] = None
) -> List[Tuple[str, str]]:
    """
    Devuelve [(COLUMN_NAME, TYPE_NAME)] ordenado por ORDINAL_POSITION para Redshift.
    Soporta "schema.table" o usa default_schema si no viene.
    """
    jvm = spark._sc._jvm
    DriverManager = jvm.java.sql.DriverManager

    props_java = jvm.java.util.Properties()
    for k, v in jprops.items():
        props_java.setProperty(k, v)

    sch = default_schema
    tname = table
    if "." in table:
        sch, tname = table.split(".", 1)

    conn = None
    rs = None
    try:
        conn = DriverManager.getConnection(url, props_java)
        meta = conn.getMetaData()
        # getColumns(null, schemaPattern, tableNamePattern, "%")
        rs = meta.getColumns(None, sch, tname, "%")
        cols = []
        while rs.next():
            name = rs.getString("COLUMN_NAME")
            tname = rs.getString("TYPE_NAME")
            pos = rs.getInt("ORDINAL_POSITION")
            cols.append((pos, name, tname))
        cols.sort(key=lambda x: x[0])
        return [(name, tname) for (_pos, name, tname) in cols]
    finally:
        try:
            if rs is not None:
                rs.close()
        finally:
            if conn is not None:
                conn.close()
