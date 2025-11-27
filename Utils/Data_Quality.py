import pandas as pd
from loguru import logger
from pathlib import Path
from typing import Dict


class DFDataQuality:
    class FileChecker:
        def __init__(self, ruta_directorio: str):
            self.ruta_directorio = Path(ruta_directorio)

        def Vef_directorio(self) -> bool:
            """
            Verifica si el directorio existe.

            Return: True si el directorio existe, False en caso contrario.
            """
            if self.ruta_directorio.is_dir():
                logger.info(f"El directorio '{self.ruta_directorio}' existe.")
                return True
            else:
                logger.critical(f"El directorio '{self.ruta_directorio}' no existe. ")
                return False

        def Verificar_archivo(self, nombre_archivo: str) -> bool:
            """
            Verifica si un archivo está presente en el directorio.

            Args:
             nombre_archivo: (str) Nombre del archivo a verificar.

            Retruns:
                True (bool) si el archivo está presente, False (bool) en caso contrario.
            """
            ruta_completa = self.ruta_directorio / nombre_archivo

            if ruta_completa.is_file():
                logger.info(
                    f"El archivo '{nombre_archivo}' está presente en el  directorio indicado."
                )
                return True
            else:
                logger.critical(
                    f"El archivo '{nombre_archivo}' no está presente en el directorio indicado."
                )
                return False

        def Cargar_df_min(self, nom_base: str, nom_hoja: str, rows=3):
            """Carga en memoria un DataFrame de pandas, unicmanete con 3 registros, para realizar pruebas que no requieran cargar todo en memoria

            Args:

               rows: (int) por defecto, 2.
            """
            if not self.Verificar_archivo(nom_base):
                raise FileNotFoundError(
                    f"El archivo '{nom_base}' no existe en el directorio '{self.ruta_directorio}'."
                )

            df_min = pd.read_excel(
                self.ruta_directorio / nom_base, sheet_name=nom_hoja, nrows=2
            )

            return df_min

        def auditar_columnas(self, df_min: pd.DataFrame, cols_necesarias: list) -> Dict:
            """Verifica la existencia de las columnas solicitadas en un df cargado

            Args:
                df_min: (pd.DataFrame)  Dataframe mínimo viable para verificar la existencia  o no de columnas.
                cols_necesarias (list): Lista de columnas a verfificar.

            Returns: Dict (Un diccionario que contiene las columnas como claves y en su valor el mensaje "Presente"/ "No presente")

            """

            cols_disponibes = df_min.columns.to_list()
            dict_cols = {}

            for cada_col in cols_necesarias:
                if cada_col in cols_disponibes:
                    dict_cols[cada_col] = "Presente"
                else:
                    dict_cols[cada_col] = "No presente"
                    
            return dict_cols
            

        def verificar_columnas(self,dict_cols):
            # Si todos los valores son "Presente"
            if all(valor == "Presente" for valor in dict_cols.values()):
                print("Todas las columnas están disponibles.")
                return True
            else:
                # Si hay alguna columna con "No presente", genera una lista con las claves correspondientes
                columnas_faltantes = [
                    col for col, estado in dict_cols.items() if estado != "Presente"
                ]
                print("Las siguientes columnas no están disponibles:")
                print(columnas_faltantes)
                return False
