# Lectura insumos
from General_Functions import Lectura_insumos_excel
from datetime import datetime
from Transformation_functions import Unidecode_solo_cols_df, filtrar_por_valores
import config_constans as cc
import pandas as pd
import os
from loguru import logger 
import Data_Quality as DQ


from config_constans import config

RUTA_INSUMOS = cc.RUTA_INSUMOS

# Constantes con los nombres de los archivos por tipo
NOM_MAESTRA_ACTIVO = cc.NOM_MAESTRA_ACTIVO
NOM_MAESTRA_INACTIVO = cc.NOM_MAESTRA_INACTIVO
NOM_BASE_SOCIOS = cc.NOM_BASE_SOCIOS
NOM_BASE_BIBLIA = cc.NOM_BASE_BIBLIA
NOM_ESTRUCTURA_VENTAS = cc.NOM_ESTRUCTURA_VENTAS

# Constantes con los nombres de las hojas para cada base
NOM_HOJA_ACTIVO = cc.NOM_HOJA_ACTIVO
NOM_HOJA_INACTIVO = cc.NOM_HOJA_INACTIVO
NOM_HOJA_SOCIOS = cc.NOM_HOJA_SOCIOS
NOM_HOJA_BIBLIA = cc.NOM_HOJA_BIBLIA
NOM_HOJA_ESTRUCTURA_VENTAS = cc.NOM_HOJA_ESTRUCTURA_VENTAS

# Numero de columnas a tomar de cada base.
COLS_ACTIVO = cc.COLS_ACTIVO
COLS_INACTIVO = cc.COLS_INACTIVO
COLS_SOCIOS = cc.COLS_SOCIOS
COLS_BIBLIA = cc.COLS_BIBLIA
COLS_ESTRUCTURA_VENTAS = cc.COLS_ESTRUCTURA_VENTAS

# Columas a selecionar de las tomadas inicialmente
COLS_ACTIVO_SELECCIONAR = cc.COLS_ACTIVO_SELECCIONAR
COLS_INACTIVO_SELECCIONAR = cc.COLS_INACTIVO_SELECCIONAR
COLS_SOCIOS_SELECCIONAR = cc.COLS_SOCIOS_SELECCIONAR
COLS_BIBLIA_SELECCIONAR = cc.COLS_BIBLIA_SELECCIONAR
COLS_ESTRUCTURA_VENTAS_SELECCIONAR = cc.COLS_ESTRUCTURA_VENTAS_SELECCIONAR
# Nota: las bases hacen referencia a cada archivo (con una hoja concreta dentro de la carpeta insumos de la automatización de seguimiento a la inversion. )

# Constantes para filtrar ( aplica solo maestras )
# Columna a filtrar
FUNC_IN = cc.FUNC_IN
# Filas sleccioandas
FUNC_IN_SELECCIONADAS = cc.FUNC_IN_SELECCIONADAS


checker = DQ.DFDataQuality.FileChecker(os.getcwd() + "\\" + RUTA_INSUMOS)
# Verificación dataQuality previo al cargue.
for cada_insumo in config["Insumos"]:
    nombre_archivo = config["Insumos"][cada_insumo]["file_name"]
    nombre_hoja = config["Insumos"][cada_insumo]["sheet"]

    archivo_existe = checker.Verificar_archivo(nombre_archivo)
    # print(f"Archivo {nombre_archivo}: {archivo_existe}")

    # 3. Cargar DataFrame mínimo con 3 registros de una hoja específica
    if archivo_existe:
        nombre_base = nombre_archivo
        nombre_hoja = nombre_hoja
        base_min = checker.Cargar_df_min(nombre_base, nombre_hoja)

    # 4. Verificar la existencia de columnas en el DataFrame
    if archivo_existe:
        dict_cols = checker.auditar_columnas(
            df_min=base_min,
            cols_necesarias=config["Insumos"][cada_insumo]["cols_verificar"],
        )
        resultado = checker.verificar_columnas(dict_cols)

respuesta_verificacion = input("¿ La verificación fue exitosa? (si/no): ")
if respuesta_verificacion.lower() == "si": 

# 1. Lectura Maestras de clientes (Activos e inactivos)
    maestra_activos = Lectura_insumos_excel(
        path=RUTA_INSUMOS,
        nom_insumo=NOM_MAESTRA_ACTIVO,
        nom_Hoja=NOM_HOJA_ACTIVO,
        cols=COLS_ACTIVO,
    )

    maestra_inactivos = Lectura_insumos_excel(
        path=RUTA_INSUMOS,
        nom_insumo=NOM_MAESTRA_INACTIVO,
        nom_Hoja=NOM_HOJA_INACTIVO,
        cols=COLS_INACTIVO,
    )

    # 2. Lectura Base socios de clientes.
    base_socios = Lectura_insumos_excel(
        path=RUTA_INSUMOS,
        nom_insumo=NOM_BASE_SOCIOS,
        nom_Hoja=NOM_HOJA_SOCIOS,
        cols=COLS_SOCIOS,
    )

    # 3. Lectura Base Estructura Ventas
    base_estructura_ventas = Lectura_insumos_excel(
        path=RUTA_INSUMOS,
        nom_insumo=NOM_ESTRUCTURA_VENTAS,
        nom_Hoja=NOM_HOJA_ESTRUCTURA_VENTAS,
        cols=COLS_ESTRUCTURA_VENTAS,
    )
   # 4. Lectura Base biblia
    base_biblia = Lectura_insumos_excel(
       path=RUTA_INSUMOS,
       nom_insumo=NOM_BASE_BIBLIA,
       nom_Hoja=NOM_HOJA_BIBLIA,
       cols=COLS_BIBLIA,
   )
   # Trasformar los nombres de las columnas a unidecode.
    maestra_activos = Unidecode_solo_cols_df(maestra_activos)
    maestra_inactivos = Unidecode_solo_cols_df(maestra_inactivos)
    base_estructura_ventas = Unidecode_solo_cols_df(base_estructura_ventas)
    base_socios = Unidecode_solo_cols_df(base_socios)
    base_biblia = Unidecode_solo_cols_df(base_biblia)

    # Seleccionar columnas necesarias.
    maestra_activos_seleccionada = maestra_activos[COLS_ACTIVO_SELECCIONAR]
    maestra_inactivos_seleccionada = maestra_inactivos[COLS_INACTIVO_SELECCIONAR]
    base_socios_seleccionada = base_socios[COLS_SOCIOS_SELECCIONAR]
    base_estructura_ventas = base_estructura_ventas[COLS_ESTRUCTURA_VENTAS_SELECCIONAR]
    base_biblia_seleccionada = base_biblia[COLS_BIBLIA_SELECCIONAR]
    base_plantilla7 = base_biblia[config["Insumos"]["Biblia"]["cols_necesarias_p7"]]

    # Filtrar solo por los jefes de ventas necesarios

    #maestra_activos_seleccionada = filtrar_por_valores(
    #    df=maestra_activos_seleccionada,
    #    columna=FUNC_IN,
    #    valores_filtrar=FUNC_IN_SELECCIONADAS,
    #)
    #maestra_inactivos_seleccionada = filtrar_por_valores(
    #    df=maestra_inactivos_seleccionada,
    #    columna=FUNC_IN,
    #    valores_filtrar=FUNC_IN_SELECCIONADAS,
    #)
else: 
    logger.info("Proceso terminado revisar insumos.")