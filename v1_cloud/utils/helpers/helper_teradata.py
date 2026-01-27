# on_cloud/utils/helpers/helper_teradata.py
from typing import Dict, List, Tuple
import re
from utils.jdbc.driver_loader import ensure_driver_loaded

# ---- Utilidades JDBC Teradata (vía JVM) ----

def _get_connection_via_driver_manager(spark, url, user, password, logger):
    # Asegura que el driver esté registrado en el DriverManager
    ensure_driver_loaded(spark, "com.teradata.jdbc.TeraDriver", logger, "helper_metadata")
    jm = spark._jvm.java.sql.DriverManager
    if user is None:
        return jm.getConnection(url)
    else:
        return jm.getConnection(url, user, password)

def build_jdbc_props(conn: Dict[str, str]) -> Dict[str, str]:
    driver = (conn.get("driver") or "com.teradata.jdbc.TeraDriver").strip()
    user = (conn.get("username") or "").strip()
    pwd = (conn.get("password") or "").strip()
    database = (conn.get("database") or "").strip()
    port = (conn.get("port") or "").strip() or "1025"
    logmech = _infer_logmech(conn)
    return {
        "driver": driver,
        "user": user,
        "password": pwd,
        "DATABASE": database,
        "DBS_PORT": port,
        "CHARSET": "UTF8",
        "LOGMECH": logmech,   # TD2/LDAP...
        "TMODE": "TERA",
        "STRICT_NAMES": "OFF",
        "batchsize": "5000",
        "isolationLevel": "NONE",
    }

def _infer_logmech(conn: Dict[str, str]) -> str:
    lm = (conn.get("logmech") or "").strip()
    if lm:
        return lm
    cu = (conn.get("connection_url") or "")
    m = re.search(r"LOGMECH\s*=\s*([A-Z0-9_]+)", cu, flags=re.IGNORECASE)
    if m:
        return m.group(1).upper()
    return "TD2"

def jdbc_url(server: str) -> str:
    if not server:
        raise ValueError("Teradata 'server' requerido.")
    return f"jdbc:teradata://{server}/"

def exec_ddl(spark, url: str, jprops: Dict[str, str], sql: str):
    jvm = spark._sc._jvm
    DriverManager = jvm.java.sql.DriverManager

    # java.util.Properties
    jprops_java = jvm.java.util.Properties()
    for k, v in jprops.items():
        jprops_java.setProperty(k, v)

    conn = None
    stmt = None
    try:
        conn = DriverManager.getConnection(url, jprops_java)
        stmt = conn.createStatement()
        stmt.execute(sql)
    finally:
        try:
            if stmt is not None:
                stmt.close()
        finally:
            if conn is not None:
                conn.close()

def fetch_table_columns(spark, url: str, jprops: Dict[str, str],
                        database: str, table: str) -> List[str]:
    """
    Retorna las columnas de Teradata en ORDEN FÍSICO (ColumnId).
    Usa DBC.ColumnsV.
    """
    jvm = spark._sc._jvm
    DriverManager = jvm.java.sql.DriverManager

    jprops_java = jvm.java.util.Properties()
    for k, v in jprops.items():
        jprops_java.setProperty(k, v)

    sql = (
        "SELECT ColumnName "
        "FROM DBC.ColumnsV "
        "WHERE DatabaseName=? AND TableName=? "
        "ORDER BY ColumnId"
    )

    cols = []
    conn = None
    ps = None
    rs = None
    try:
        conn = DriverManager.getConnection(url, jprops_java)
        ps = conn.prepareStatement(sql)
        ps.setString(1, database)
        ps.setString(2, table)
        rs = ps.executeQuery()
        while rs.next():
            cols.append(rs.getString(1))
    finally:
        try:
            if rs is not None: rs.close()
        finally:
            try:
                if ps is not None: ps.close()
            finally:
                if conn is not None: conn.close()
    return cols

def split_db_table(qualified: str, default_db: str) -> Tuple[str, str]:
    """
    'DB.TABLA' -> ('DB','TABLA'); 'TABLA' -> (default_db,'TABLA')
    """
    q = (qualified or "").strip()
    if "." in q:
        db, tb = q.split(".", 1)
        return db.strip(), tb.strip()
    return (default_db.strip(), q)
