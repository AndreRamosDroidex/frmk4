from cryptography.hazmat.primitives import hashes, padding
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
import base64
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from utils.others.helper_functions import get_name_function

# --- Configuración de claves y cifrado ---
key = "1bcebea2cb3629bab79b9dc51c7e984ee51ee436d88f4e8ec38c41dfa15d71f2"
salt = b'salt_BANKINTER'
iv = b'int-frmk4-ocsd98'

# Derivar clave a partir de la clave base y salt
kdf = PBKDF2HMAC(
    algorithm=hashes.SHA256(),
    length=32,
    salt=salt,
    iterations=100000
)
derivedKey = kdf.derive(key.encode())

# --- Cifrado base AES-CBC ---
cipher = Cipher(algorithms.AES(derivedKey), modes.CBC(iv))


# ===============================
# ENCRIPTAR UN VALOR
# ===============================
def encrypt_field(plaintext: str) -> str:
    try:
        if plaintext is None:
            return None
        encryptor = cipher.encryptor()
        padder = padding.PKCS7(128).padder()
        padded_data = padder.update(plaintext.encode()) + padder.finalize()
        ciphertext = encryptor.update(padded_data) + encryptor.finalize()
        return base64.b64encode(ciphertext).decode('utf-8')
    except Exception as e:
        print(f"[ERROR] encrypt_field - Valor: {plaintext} - Error: {e}")
        return None


# ===============================
# DESENCRIPTAR UN VALOR
# ===============================
def decrypt_field(encrypted_text: str) -> str:
    try:
        if encrypted_text is None:
            return None
        decryptor = cipher.decryptor()
        ciphertext = base64.b64decode(encrypted_text.encode())
        padded_plaintext = decryptor.update(ciphertext) + decryptor.finalize()
        unpadder = padding.PKCS7(128).unpadder()
        plaintext = unpadder.update(padded_plaintext) + unpadder.finalize()
        return plaintext.decode('utf-8')
    except Exception as e:
        print(f"[ERROR] decrypt_field - Valor: {encrypted_text} - Error: {e}")
        return None


# ===============================
# ENCRIPTAR COLUMNAS EN SPARK
# ===============================
def encrypt_dataframe(df, columns_to_encrypt, config, log):
    """
    Encripta las columnas especificadas en un DataFrame de Spark,
    devolviendo un nuevo DataFrame sin modificar el original.
    """
    
    # Obtener el nombre de esta funcion para usarlo en los logs
    name_function = get_name_function()
    log.registrar("INFO",f"[{name_function}] - Iniciando encrypt")
    
    # Creamos la funcion udf
    encrypt_udf = udf(encrypt_field, StringType())
    
    # Creamos una copia del DataFrame base
    new_df = df

    for col_name in columns_to_encrypt:
        if col_name in new_df.columns:
            # Generamos una nueva columna con sufijo '_enc' (por claridad)
            new_df = new_df.withColumn(col_name, encrypt_udf(col(col_name)))
        else:
            log.registrar("WARNING",f"[{name_function}] - Columna '{col_name}' no encontrada en el DataFrame.")

    return new_df


# ===============================
# DESENCRIPTAR COLUMNAS EN SPARK
# ===============================
def decrypt_dataframe(df, columns_to_decrypt, config, log):
    """
    Desencripta las columnas especificadas en un DataFrame de Spark.

    Parámetros:
        df (DataFrame): DataFrame de entrada.
        columns_to_decrypt (list): Lista de columnas a desencriptar.

    Retorna:
        DataFrame de Spark con los campos desencriptados.
    """
    
    # Obtener el nombre de esta funcion para usarlo en los logs
    name_function = get_name_function()
    log.registrar("INFO",f"[{name_function}] - Iniciando decrypt")
    
    decrypt_udf = udf(decrypt_field, StringType())
    
    # Creamos una copia del DataFrame base
    new_df = df

    for col_name in columns_to_decrypt:
        if col_name in new_df.columns:
            new_df = new_df.withColumn(col_name, decrypt_udf(col(col_name)))
        else:
            log.registrar("WARNING",f"[{name_function}] - Columna '{col_name}' no encontrada en el DataFrame.")
            
    log.registrar("INFO",f"[{name_function}] - Finalizado decrypt_dataframe")
    return new_df
