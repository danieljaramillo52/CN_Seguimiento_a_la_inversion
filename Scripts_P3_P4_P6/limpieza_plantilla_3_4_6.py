# Lectura de archivos necesarios para la plantilla N°3
from datetime import datetime
from General_Functions import Lectura_insumos_excel, exportar_a_excel
from Transformation_functions import (
    Renombrar_columnas_con_diccionario,
    concatenar_columnas,
    concatenar_dataframes,
    Unidecode_solo_cols_df,
    Eliminar_columnas,
    Eliminar_primeras_n_filas,
    Eliminar_primeros_n_caracteres,
    duplicar_columna,
    Agregar_columna_constante,
    Agregar_columnas_constantes,
    pd_left_merge_two_keys,
    Unidecode_solo_cols_df,
    Formatear_primera_letra_mayuscula,
)
import pandas as pd
import config_constans as cc
import math

config = cc.config

RUTAS_INSUMOS = config["RUTAS_INSUMOS"]
DICT_MESES = config["dict_meses"]

a_a_a_a = "Año"
col_valor_condicionados = "Dcto Condicionados (SC) Acum Año Actual $"
col_valor_pie_factura = "Descuentos Pie de Factura (SC) Acum Ano Actual $"

RUTA_DRIVERS = cc.RUTA_DRIVERS
NOM_DRIVER_USUARIO = cc.NOM_DRIVER_USUARIO
DRIVERS_USUARIO = cc.DRIVERS_USUARIO
COLS_RENOMBRAR_PLANTILLA_3_ACT = cc.COLS_RENOMBRAR_PLANTILLA_3_ACT

DICT_COLS_BO = config["Dict_columnas_bo"]
DESCUENTO_SOMBRILLA = config["DESCUENTO_SOMBRILLA"]
DESCUENTO_CONCEPTO = config["DESCUENTO_CONCEPTO"]

COD_CP = config["COD_CP"]
CODIGO = config["CODIGO"]
COD_CLI_ORIG = config["COD_CLI_ORIG"]
MES = config["MES"]
MES_ORIG = config["MES_ORIG"]
VALOR = config["VALOR"]
AGRUPACION_ACTUAL = config["AGRUPACION_ACTUAL"]
DESCRIPCION_CONCEPTO = config["DESCRIPCION_CONCEPTO"]
NOM_COL_P6 = config["MODS_COLS_PLANTILLA_6"]

COLS_SELECCIONAR_AFOS_P3 = config["Insumos_P3"]["AFO_Depuraciones"][
    "cols_seleccionar_afos_p3"
]
RENOMBRAR_COLS_PIE_FACTURA = config["Insumos_P3"]["AFO_Depuraciones"][
    "renombrar_cols_pie_factura"
]
RESULTADOS_P3 = cc.RESULTADOS_P3

# Lectura bases AFO Plantilla N°3

afo_condicionados = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTAS_INSUMOS["Insumos_P3"],
        nom_insumo=config["Insumos_P3"]["AFO_Depuraciones"]["file_name"],
        nom_Hoja=config["Insumos_P3"]["AFO_Depuraciones"]["sheet"][0],
        cols=config["Insumos_P3"]["AFO_Depuraciones"]["cols"][0],
    )
)

afo_pie_factura = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTAS_INSUMOS["Insumos_P3"],
        nom_insumo=config["Insumos_P3"]["AFO_Depuraciones"]["file_name"],
        nom_Hoja=config["Insumos_P3"]["AFO_Depuraciones"]["sheet"][1],
        cols=config["Insumos_P3"]["AFO_Depuraciones"]["cols"][1],
    )
)

# Lectura historico Plantilla N°3
p3_historico = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTAS_INSUMOS["Insumos_P3"],
        nom_insumo=config["Insumos_P3"]["P3_Historico"]["file_name"],
        nom_Hoja=config["Insumos_P3"]["P3_Historico"]["sheet"],
        cols=config["Insumos_P3"]["P3_Historico"]["cols"],
    )
)
# Renombrar columna por el modo de estructura de columnas en la lectrua de archivos
# Lectura Base_BO para la plantilla N°4
base_pto_inver_x_cliente = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTAS_INSUMOS["Insumos_P4"],
        nom_insumo=config["Insumos_P4"]["BO_Ppto_inverision_cliente"]["file_name"],
        nom_Hoja=config["Insumos_P4"]["BO_Ppto_inverision_cliente"]["sheet"],
        cols=config["Insumos_P4"]["BO_Ppto_inverision_cliente"]["cols"],
    )
)

# Crear una copia para manipulacion.
base_pto_inver_x_cliente_copy = base_pto_inver_x_cliente.copy()

# Lectura de los archivos pra la plantilla 6.
# Lectura Base BO_envios_directos
bo_envios_directos = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTAS_INSUMOS["Insumos_P6"],
        nom_insumo=config["Insumos_P6"]["BO_env_direct_cont_devs"]["file_name"],
        nom_Hoja=config["Insumos_P6"]["BO_env_direct_cont_devs"]["sheet"][0],
        cols=config["Insumos_P6"]["BO_env_direct_cont_devs"]["cols"][0],
    )
)
bo_contado = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTAS_INSUMOS["Insumos_P6"],
        nom_insumo=config["Insumos_P6"]["BO_env_direct_cont_devs"]["file_name"],
        nom_Hoja=config["Insumos_P6"]["BO_env_direct_cont_devs"]["sheet"][1],
        cols=config["Insumos_P6"]["BO_env_direct_cont_devs"]["cols"][1],
    )
)
bo_devs_malas = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTAS_INSUMOS["Insumos_P6"],
        nom_insumo=config["Insumos_P6"]["BO_env_direct_cont_devs"]["file_name"],
        nom_Hoja=config["Insumos_P6"]["BO_env_direct_cont_devs"]["sheet"][2],
        cols=config["Insumos_P6"]["BO_env_direct_cont_devs"]["cols"][2],
    )
)
# Lectura Driver_plantilla_2 == Archivo generado en Plantilla_2_p2
driver_plantilla_2 = Unidecode_solo_cols_df(Lectura_insumos_excel(
    path=config["path"]["Resultados"]["RESULTADOS_P1"],
    nom_insumo=config["Plantillas"]["Plantilla1"]["noms"][0] + ".xlsx",
    nom_Hoja=config["Plantillas"]["Plantilla1"]["noms"][0],
    cols=22,
))
# Lectura driver_dcto_cond_x_concep
driver_dcto_cond_x_concep = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_USUARIO,
        nom_Hoja=DRIVERS_USUARIO[0],
        cols=5,
    )
)
# Lectura driver dcto_envio_directo
driver_dcto_envio_directo = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_USUARIO,
        nom_Hoja=DRIVERS_USUARIO[8],
        cols=3,
    )
)

# Selecion de datos de la biblia plantilla 7


# Procedimientos.  ARCHIVOS PLANTILLA NUMERO 3 (afo_condicionados y afo_pie_factura , P3Historico)

afo_condicionados = Renombrar_columnas_con_diccionario(
    base=afo_condicionados,
    cols_to_rename=config["Insumos_P3"]["AFO_Depuraciones"]["renombrar_cols"],
)
afo_pie_factura = Renombrar_columnas_con_diccionario(
    base=afo_pie_factura,
    cols_to_rename=RENOMBRAR_COLS_PIE_FACTURA,
)

afo_pie_factura = Formatear_primera_letra_mayuscula(
    dataframe=afo_pie_factura, columna=MES
)

afo_condicionados_rename = Eliminar_primeras_n_filas(df=afo_condicionados, n=1)
afo_pie_factura_rename = Eliminar_primeras_n_filas(df=afo_pie_factura, n=1)

# Modificaciones para AFO Condicionados.
afo_condicionados_rename = duplicar_columna(
    df=afo_condicionados_rename.copy(),
    columna_a_duplicar=DESCUENTO_SOMBRILLA,
    columna_nueva=f"{DESCUENTO_SOMBRILLA}_recortado",
)

afo_condicionados_rename = Eliminar_primeros_n_caracteres(
    df=afo_condicionados_rename.copy(), columna=f"{DESCUENTO_SOMBRILLA}_recortado", n=5
)

# Agregar un (0) inicial a todos los elementos de la columna "Descuento concepto" donde las cadenas de cada registro de la columna, contengan numeros menores a 10.
afo_condicionados_rename[DESCUENTO_CONCEPTO] = afo_condicionados_rename[
    DESCUENTO_CONCEPTO
].apply(
    lambda cada_fila: (
        f"-0{cada_fila}"
        if cada_fila in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]
        else f"-{cada_fila}"
    )
)
# Reconvertir la columna en string.
# afo_condicionados_rename[DESCUENTO_CONCEPTO] = afo_condicionados_rename[
#    DESCUENTO_CONCEPTO
# ].astype(str)

afo_condicionados_rename[MES] = afo_condicionados_rename[MES].apply(lambda x: x.title())

afo_condicionados_def = concatenar_columnas(
    dataframe=afo_condicionados_rename,
    cols_elegidas=[f"{DESCUENTO_SOMBRILLA}_recortado", DESCUENTO_CONCEPTO],
    nueva_columna=COD_CP,
)

afo_condicionados_def = afo_condicionados_def[COLS_SELECCIONAR_AFOS_P3]
afo_pie_factura_rename = afo_pie_factura_rename[COLS_SELECCIONAR_AFOS_P3]

# Concatenar ambos dataframes plantilla_3.
afo_condicionados_y_pie_factura = concatenar_dataframes(
    df_list=[afo_condicionados_def, afo_pie_factura_rename]
)

# Agregar columnas calculadas usando (driver_dcto_cond_x_concep y Driver_plantilla_2 )
# Generamos una copia antes de hacer multiples merge.
afo_condicionados_y_pie_factura_copy = afo_condicionados_y_pie_factura.copy()

afo_condicionados_y_pie_factura_copy2 = pd_left_merge_two_keys(
    base_left=afo_condicionados_y_pie_factura_copy,
    base_right=driver_dcto_cond_x_concep[
        [CODIGO, DESCRIPCION_CONCEPTO, AGRUPACION_ACTUAL]
    ].drop_duplicates(),
    left_key=COD_CP,
    right_key=CODIGO,
)
afo_condicionados_y_pie_factura_copy2 = Eliminar_columnas(
    df=afo_condicionados_y_pie_factura_copy2, columnas_a_eliminar=[CODIGO]
)
afo_condicionados_y_pie_factura_copy2_rename = Renombrar_columnas_con_diccionario(
        base=afo_condicionados_y_pie_factura_copy2, cols_to_rename=COLS_RENOMBRAR_PLANTILLA_3_ACT
    )

# Agregamos una columna constante con el año actual, para las plantillas 3 y 4.
afo_condicionados_y_pie_factura_copy2_rename = Agregar_columna_constante(
    afo_condicionados_y_pie_factura_copy2_rename,
    nombre_col_cols=a_a_a_a,
    valor_constante=str(datetime.now().year),
)

p3_historico_rename = Renombrar_columnas_con_diccionario(
        base=p3_historico, cols_to_rename=COLS_RENOMBRAR_PLANTILLA_3_ACT
    )

# Actualizamos el historico de la plantilla 3 para el proximo mes.
p3_historico_act = concatenar_dataframes(
    df_list=[
        p3_historico_rename,
        afo_condicionados_y_pie_factura_copy2_rename,
    ]
)
#Eliminar duplicados presentes en el historico. 
p3_historico_act_sin_dup = p3_historico_act.drop_duplicates()

# Nuevo Historico P3_Historico.xlsx
exportar_a_excel(
    df=p3_historico_act_sin_dup,
    ruta_guardado=RESULTADOS_P3,
    nom_hoja=config["Insumos_P3"]["P3_Historico"]["sheet"],
)

# Procedimientos plantilla 4. (base_pto_inver_x_cliente_copy)
base_pto_inver_x_cliente_copy = Agregar_columna_constante(
    base_pto_inver_x_cliente_copy,
    nombre_col_cols=a_a_a_a,
    valor_constante=str(datetime.now().year),
)

base_pto_inver_x_cliente_copy = Renombrar_columnas_con_diccionario(
    base=base_pto_inver_x_cliente_copy, cols_to_rename=DICT_COLS_BO
)

base_pto_inver_x_cliente_copy = Agregar_columna_constante(
    base_pto_inver_x_cliente_copy,
    nombre_col_cols=MES_ORIG,
    valor_constante=a_a_a_a,
)
# Renombramos una columna conocida como sector (Que contiene los negocios a negocio para
# evitar duplicidad de nombres de columnas mas adelante.)
base_pto_inver_x_cliente_copy = Renombrar_columnas_con_diccionario(
    base=base_pto_inver_x_cliente_copy, cols_to_rename={"Sector": "Negocio"}
)
# Plantilla N°6 Limpieza.
# Agregar columnas constantes para estandarizar la informacion.
bo_contado_mod = Agregar_columnas_constantes(
    dataframe=bo_contado, columnas_valores=NOM_COL_P6["COLS_AGREGAR_PLANTILLA6_CONTADO"]
)

bo_devs_malas_mod = Agregar_columnas_constantes(
    dataframe=bo_devs_malas,
    columnas_valores=NOM_COL_P6["COLS_AGREGAR_PLANTILLA6_DEV_MAL"],
)

# Reordenar las columnas de las bases.
bo_contado_mod = bo_contado_mod[NOM_COL_P6["COLS_REORDERNAR_PLANTILLA6_CONT_DEV"]]

bo_devs_malas_mod = bo_devs_malas_mod[NOM_COL_P6["COLS_REORDERNAR_PLANTILLA6_CONT_DEV"]]

# Concatenar ambas bases
# bd_ed_dm_cont == base_driver_ed_devoluciones_malas_contado
bd_plantilla6 = concatenar_dataframes([bo_contado_mod, bo_devs_malas_mod])

# Renombrar columnas.
bd_plantilla6_rename = Renombrar_columnas_con_diccionario(
    base=bd_plantilla6, cols_to_rename=NOM_COL_P6["COLS_RENOMBRAR_PLANTILLA6_CONCAT"]
)
