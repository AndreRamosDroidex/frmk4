from utils.others.helper_functions import get_name_function

class LoadRedshift:
    def __init__(self, spark, config, logger):
        self.spark = spark
        self.config = config
        self.logger = logger

    def run(self, df, props: dict):

        # Obtener el nombre de esta funcion para usarlo en los logs
        name_function = get_name_function()

        # Destino
        self.origen = {
            "local-new-egde": False,
            "local-ec2": True,
            "s3": False
        }        
        
        if self.origen.get("local-new-egde", False):
            # New Egde
            path = "/data/desa/IBKProjects/DataHub/frmk4/tmp/outputs/output_redshift/"
        elif self.origen.get("local-ec2", False):
            # AWS - EC2
            path = "/home/ubuntu/frmk4/tmp/outputs/output_redshift/"
        elif self.origen.get("s3", False):
            # AWS - s3
            path = f"s3://.../data.output_redshift/"
        
        self.logger.registrar("INFO", f"[{name_function}] - (POC) Guardando resultados Redshift en: {path}")
        df.write.mode("overwrite").parquet(path)

        #usar COPY a Redshift con un stage en S3 usando props
        #data_connection, database, schema, table
