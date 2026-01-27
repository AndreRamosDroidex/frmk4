# utils/db/helper_teradata.py
from typing import List, Tuple
from pyspark.sql import SparkSession

def get_table_columns_order(
    spark: SparkSession,
    url: str,
    jprops: dict,
    table: str
) -> List[Tuple[str, str]]:
    """

    """
    jvm = spark._sc._jvm
    DriverManager = jvm.java.sql.DriverManager

    # Properties java
    props_java = jvm.java.util.Properties()
    for k, v in jprops.items():
        props_java.setProperty(k, v)

    schema = None
    tname = table
    if "." in table:
        schema, tname = table.split(".", 1)

    conn = None
    rs = None
    try:
        conn = DriverManager.getConnection(url, props_java)
        meta = conn.getMetaData()

        # getColumns(null, schema, table, "%")
        rs = meta.getColumns(None, schema, tname, "%")
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
