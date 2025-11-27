from General_Functions import (
    leer_carpeta_de_archivos_excel,
    Lectura_insumos_excel,
    Eliminar_acentos,
    exportar_a_excel,
)
from Transformation_functions import (
    Unidecode_solo_cols_df,
    duplicar_columna,
    Renombrar_columnas_con_diccionario,
    Reemplazar_columna_en_funcion_de_otra,
    Group_by_and_sum_cols,
    reemplazar_indices,
    Eliminar_filas_duplicadas,
    pd_left_merge_two_keys,
    pd_left_merge,
    concatenar_dataframes,
    concatenar_columnas,
    Cambiar_tipo_dato_multiples_columnas,
    Formatear_primera_letra_mayuscula,
    Filtrar_por_valores_excluidos,
    seleccionar_columnas,
    filtrar_por_valores,
)

import config_constans as cc

config = cc.config
NIF = cc.NIF
NIF_ORIG = cc.NIF_ORIG
MES = cc.MES
MES_ORIG = cc.MES_ORIG
RUTA_BASES_BO = cc.RUTA_BASES_BO
RUTA_DRIVERS = cc.RUTA_DRIVERS
NUEVAS_COLS_BO = cc.NUEVAS_COLS_BO
COD_LOC = cc.COD_LOC
COD_CLI = cc.COD_CLI
COD_OV = cc.COD_OV
CLIENTE = cc.CLIENTE
FIL_SIN_ASIG = cc.FIL_SIN_ASIG
FIL_NUMERAL = cc.FIL_NUMERAL
COD_CLI_ORIG = cc.COD_CLI_ORIG
RAZON_SOCIAL = cc.RAZON_SOCIAL
RAZON_SOCIAL_LOC = cc.RAZON_SOCIAL_LOC
RAZON_SOCIAL_ORIG = cc.RAZON_SOCIAL_ORIG
REGION = cc.REGION
NOMBRE_COMERCIAL = cc.NOMBRE_COMERCIAL
NOMBRE_COMERCIAL_ORIG = cc.NOMBRE_COMERCIAL_ORIG
NOMBRE_COMERCIAL_LOC = cc.NOMBRE_COMERCIAL_LOC
CN_TIP_ORIG = cc.CN_TIP_ORIG
SCN_TIP_ORIG = cc.SCN_TIP_ORIG
TIP_ORIG = cc.TIP_ORIG
CN_TIP_LOC = cc.CN_TIP_LOC
SCN_TIP_LOC = cc.SCN_TIP_LOC
TIP_LOC = cc.TIP_LOC
CLAVE = cc.CLAVE
DCTO_ANT = cc.DCTO_ANT
CANAL_TIP = cc.CANAL_TIP
SUB_CANAL_TIP = cc.SUB_CANAL_TIP
SEGMENTO = cc.SEGMENTO
CONCATENADAS = cc.CONCATENADAS
FORMATO_ORIG = cc.FORMATO_ORIG
PLAN_DE_NEGOCIO = cc.PLAN_DE_NEGOCIO
GRANDES_CADENAS = cc.GRANDES_CADENAS
AGRUPACION_CLIENTES = cc.AGRUPACION_CLIENTES
NOM_DRIVER_GENERADO = cc.NOM_DRIVER_GENERADO
NOM_DRIVER_USUARIO = cc.NOM_DRIVER_USUARIO
DRIVERS_USUARIO = cc.DRIVERS_USUARIO
DRIVERS_GENERADOS = cc.DRIVERS_GENERADOS
FORMATO = cc.FORMATO
COD_KAM = cc.COD_KAM
NOMBRE_KAM = cc.NOMBRE_KAM
COD_JV = cc.COD_JV
COD_RV = cc.COD_RV
COD_FC = cc.COD_FC
VENDEDOR = cc.VENDEDOR
JEFE_VENTAS = cc.JEFE_VENTAS
CANAL_TRANS = cc.CANAL_TRANS
TIPO_INVERSION = cc.TIPO_INVERSION
SUB_CANAL_TRANS = cc.SUB_CANAL_TRANS
SEGMENTO_TRANS = cc.SEGMENTO_TRANS
ESCALAS = cc.ESCALAS
OFICINA_VENTAS = cc.OFICINA_VENTAS
NOMBRE_FIGURA_COMERCIAL = cc.NOMBRE_FIGURA_COMERCIAL
N_CLIENTE = cc.N_CLIENTE
CANAL_EST = cc.CANAL_EST
SUB_CANAL_EST = cc.SUB_CANAL_EST
DICT_CN_TIP_GC = cc.DICT_CN_TIP_GC
DICT_SUBCN_TIP_TI_DROG = cc.DICT_SUBCN_TIP_TI_DROG
SOCIOS = cc.SOCIOS
RESULTADOS_P1 = cc.RESULTADOS_P1
FIL_GUION = cc.FIL_GUION
FIL_NO = cc.FIL_NO
COLS_AGRUP_PLANTILLA1_TD_D_CLI = cc.COLS_AGRUP_PLANTILLA1_TD_D_CLI
COLS_AGRUP_PLANTILLA1_TD_VT_DTCOS = cc.COLS_AGRUP_PLANTILLA1_TD_VT_DTCOS
COLS_SUM_PLANTILLA1_TD_VD = cc.COLS_SUM_PLANTILLA1_TD_VD

# 0. Lectura de Drivers

driver_clientes_plan_negocios = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_GENERADO,
        nom_Hoja=DRIVERS_GENERADOS[0],
        cols=5,
    )
)

driver_clientes_socios = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_GENERADO,
        nom_Hoja=DRIVERS_GENERADOS[1],
        cols=5,
    )
)
driver_nombre_figura_comercial = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_GENERADO,
        nom_Hoja=DRIVERS_GENERADOS[2],
        cols=2,
    )
)
driver_cods_jv_y_rv = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_GENERADO,
        nom_Hoja=DRIVERS_GENERADOS[3],
        cols=4,
    )
)
driver_estructura_de_ventas = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_GENERADO,
        nom_Hoja=DRIVERS_GENERADOS[4],
        cols=9,
    )
)
driver_clientes_nit_maestra = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_GENERADO,
        nom_Hoja=DRIVERS_GENERADOS[5],
        cols=2,
    )
)
driver_dcto_cond_x_concep = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_USUARIO,
        nom_Hoja=DRIVERS_USUARIO[0],
        cols=5,
    )
)
driver_jerarquia_cad = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_USUARIO,
        nom_Hoja=DRIVERS_USUARIO[1],
        cols=4,
    )
)
driver_region = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_USUARIO,
        nom_Hoja=DRIVERS_USUARIO[2],
        cols=3,
    )
)
driver_jerarquia_comer = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_USUARIO,
        nom_Hoja=DRIVERS_USUARIO[3],
        cols=10,
    )
)
driver_kam_x_formato_region = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_USUARIO,
        nom_Hoja=DRIVERS_USUARIO[4],
        cols=5,
    )
)
driver_escalas = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_USUARIO,
        nom_Hoja=DRIVERS_USUARIO[5],
        cols=5,
    )
)

driver_clientes_medibles = Unidecode_solo_cols_df(
    Lectura_insumos_excel(
        path=RUTA_DRIVERS,
        nom_insumo=NOM_DRIVER_GENERADO,
        nom_Hoja=DRIVERS_GENERADOS[6],
        cols=1,
    )
)
# 1. Lectura Base BO seguimiento a inversion Acumulado
# Lectura todas las bases de BO.
lista_bases_bo = leer_carpeta_de_archivos_excel(RUTA_BASES_BO)
# Concatenar las bases BO leidas.
base_concat_bo = concatenar_dataframes(lista_bases_bo)
# Pasamos a formato unidecode.
base_cocat_bo = Unidecode_solo_cols_df(base_concat_bo)
# Creamos una copia sobre la cual hacer todas las transformaciones.
bases_bo = base_concat_bo.copy()

# Tomar los valores faltantes.


base_bo_rename = Renombrar_columnas_con_diccionario(
    base=bases_bo, cols_to_rename=NUEVAS_COLS_BO
)

# Selecionar subconjuntos necesarios de los drivers.
# Nota: Driver => archivos *xlsx/*xlms (Que se convirtieron en datafames).
cliente_nit_maestra_mod = Eliminar_filas_duplicadas(
    driver_clientes_nit_maestra[[CLIENTE, NIF]]
)
driver_extraer_region = Eliminar_filas_duplicadas(driver_region[[COD_OV, REGION]])

driver_extraer_canal_SubCanal_Tip_agrup = Eliminar_filas_duplicadas(
    driver_jerarquia_comer[[CLAVE, CANAL_TRANS, SUB_CANAL_TRANS, SEGMENTO_TRANS]]
)
driver_segmento_gc = Eliminar_filas_duplicadas(
    driver_jerarquia_cad[[FORMATO_ORIG, SEGMENTO]]
)
driver_agrup_clientes_gc = Eliminar_filas_duplicadas(
    driver_jerarquia_cad[[FORMATO_ORIG, AGRUPACION_CLIENTES]]
)
driver_formato_clientes_gc = Eliminar_filas_duplicadas(
    driver_jerarquia_cad[[FORMATO_ORIG, FORMATO]]
)
driver_kam_formato_cod_jv = Eliminar_filas_duplicadas(
    driver_kam_x_formato_region[[CLAVE, COD_KAM, NOMBRE_KAM]]
)
driver_estr_de_vtas_cn_subcn_est = Eliminar_filas_duplicadas(
    driver_estructura_de_ventas[[COD_RV, CANAL_EST, SUB_CANAL_EST]]
).drop_duplicates(subset=COD_RV)
# Creamos una copia sobre la cual hacer todas las transformaciones.

# COLUMNA MES.

# 1.) Duplicar la columna MES_ORIG de bases_bo_rename y almacenarla con la constante MES
base_bo = duplicar_columna(df=base_bo_rename, columna_a_duplicar=MES_ORIG, columna_nueva=MES)


# COLUMNA COD_CLI

# 1.) Duplicar la columna COD_LOC y almacenarla con la constante COD_CLI
base_bo = duplicar_columna(
    df=base_bo, columna_a_duplicar=COD_CLI_ORIG, columna_nueva=COD_CLI
)
base_bo.loc[:,COD_LOC] = base_bo[COD_CLI]
#Modificación número 2. #OJOOOO

# 2.) traer los indices donde los cod. loc. == #.
#indices_numeral_cod_loc = base_bo[base_bo[COD_LOC].isin([FIL_NUMERAL])].index

# 3.) Reemplazar los índices seleccionados, según los valores de la columna COD_CLI_ORIG, en la columna COD_CLI
#base_bo = reemplazar_indices(
#    df=base_bo,
#    columna_a_reemplazar=COD_CLI,
#    columna_con_valores=COD_CLI_ORIG,
#    indices=indices_numeral_cod_loc,
#)


# COLUMNA NIF

# 1) Traer la columna Nit de Driver Cliente-Nit-maestra.
base_bo = pd_left_merge_two_keys(
    base_left=base_bo,
    base_right=cliente_nit_maestra_mod,
    left_key=COD_CLI,
    right_key=CLIENTE,
)
# 2) Rellenar la columna Nit con "#"
base_bo[NIF] = base_bo[NIF].fillna(FIL_NUMERAL)

# COLUMNA RAZON SOCIAL

# 1). Duplicar la columna RAZON_SOCIAL_LOC y almacenarla con la constante RAZON_SOCIAL
base_bo = duplicar_columna(
    df=base_bo, columna_a_duplicar=RAZON_SOCIAL_LOC, columna_nueva=RAZON_SOCIAL
)

# 2.) Traer los indices donde razon_social_loc. == #.
indices_sin_asig_raz_social = base_bo[
    base_bo[RAZON_SOCIAL_LOC].isin([FIL_SIN_ASIG])
].index

# 3.) Reemplazar los índices seleccionados, según los valores de la columna RAZON_SOCIAL_ORIG, en la columna RAZON_SOCIAL
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=RAZON_SOCIAL,
    columna_con_valores=RAZON_SOCIAL_ORIG,
    indices=indices_sin_asig_raz_social,
)

# COLUMNA NOMBRE COMERCIAL

# 1.) Duplicar la columna NOMBRE_COMERCIAL_LOC y almacenarla con la constante NOMBRE_COMERCIAL
base_bo = duplicar_columna(
    df=base_bo, columna_a_duplicar=NOMBRE_COMERCIAL_LOC, columna_nueva=NOMBRE_COMERCIAL
)

# 2.) # Traer los indices donde donde Nombre_comercial_loc == "#"
indices_numeral_nom_comercial = base_bo[
    base_bo[NOMBRE_COMERCIAL_LOC].isin([FIL_NUMERAL])
].index

# 3.) # Reemplazar los índices seleccionados, según los valores de la columna NOMBRE_COMERCIAL_ORIG, en la columna NOMBRE_COMERCIAL
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=NOMBRE_COMERCIAL,
    columna_con_valores=NOMBRE_COMERCIAL_ORIG,
    indices=indices_numeral_nom_comercial,
)

# Aplicamos función que replica NOM_PROPIO en excel a varias columnas.
base_bo = Formatear_primera_letra_mayuscula(dataframe=base_bo, columna=MES)
base_bo = Formatear_primera_letra_mayuscula(dataframe=base_bo, columna=RAZON_SOCIAL)
base_bo = Formatear_primera_letra_mayuscula(dataframe=base_bo, columna=NOMBRE_COMERCIAL)

# COLUMNA REGION

# 1). Traer la columna Region de los drivers.
base_bo = pd_left_merge(base_left=base_bo, base_right=driver_extraer_region, key=COD_OV)


# COLUMNAS Canal_Tip , Sub_Canal_Tio, y Segmento.

# 1.) Concatena las columnas en la lista cols_elegidas para agregar la concatenacion de los valores como una nueva columna.
base_bo = concatenar_columnas(
    dataframe=base_bo,
    cols_elegidas=[CN_TIP_LOC, SCN_TIP_LOC, TIP_LOC],
    nueva_columna=CONCATENADAS[0],
)
base_bo = concatenar_columnas(
    dataframe=base_bo,
    cols_elegidas=[CN_TIP_ORIG, SCN_TIP_ORIG, TIP_ORIG],
    nueva_columna=CONCATENADAS[1],
)
# 2.) Con la ayuda de la clave de las nuevas columnas concatenadas. Trae dos veces las columnas necesarias.
for col_concatenada in CONCATENADAS:
    base_bo = pd_left_merge_two_keys(
        base_left=base_bo,
        base_right=driver_extraer_canal_SubCanal_Tip_agrup,
        left_key=col_concatenada,
        right_key=CLAVE,
    )
    # Eliminar la columna CLAVE para evitar duplicidad.
    base_bo = base_bo.copy().drop([CLAVE], axis=1)

# 3.) Definir los indices a reemplazar
indices_nulos_Canal_Tip = base_bo[base_bo[f"{CANAL_TRANS}_x"].isnull()].index
indices_nulos_SubCanal_Tip = base_bo[base_bo[f"{SUB_CANAL_TRANS}_x"].isnull()].index
indices_nulos_Tipologia = base_bo[base_bo[f"{SEGMENTO_TRANS}_x"].isnull()].index

# Estamos tomando los indices vacios en f"{CANAL_TRANS_AJUST}_x" los que no cruzaron. Y estamos reemplazando los valores por los del segudo cruce. Para canal y sub canal y Tip
# 4.) Reemplazar los indices

base_bo = reemplazar_indices(
    base_bo,
    columna_a_reemplazar=f"{CANAL_TRANS}_x",
    columna_con_valores=f"{CANAL_TRANS}_y",
    indices=indices_nulos_Canal_Tip,
)
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=f"{SUB_CANAL_TRANS}_x",
    columna_con_valores=f"{SUB_CANAL_TRANS}_y",
    indices=indices_nulos_SubCanal_Tip,
)
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=f"{SEGMENTO_TRANS}_x",
    columna_con_valores=f"{SEGMENTO_TRANS}_y",
    indices=indices_nulos_Tipologia,
)

# 5.) Traemos la condicion de grandes cadenas, para reemplazar la informacion flatante en los subfijos _x , con la información complementaria de los subfijos _y
indices_remplazar_gc = base_bo[
    base_bo[f"{CANAL_TRANS}_x"].isin([GRANDES_CADENAS])
].index


# 6.) Tambien vamos a traer solo los que son grandes cadenas en segmento. Notemos segmento es como debe quedar el nombre de la columna f"{SEGMENTO_TRANS}_x" cuando hablamos de segmento y segmento_trans, en la misma columna vamos a guardar los reemplazos.
base_bo = pd_left_merge(
    base_left=base_bo,
    base_right=driver_segmento_gc,
    key=FORMATO_ORIG,
)

# 7.) luego vamos a reemplazar los indices correspondientes a grandes cadenas En la columna  TIPOLOGIA_AJUST_x que será la llamada SEGMENTO , para la Base BO
# Recordar : El nombre TIPOLOGIA_AJUST es el nombre con el que viene la columna en el driver.
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=f"{SEGMENTO_TRANS}_x",
    columna_con_valores=FORMATO_ORIG,
    indices=indices_remplazar_gc,
)

# 7.) Eliminamos las columnas que no son necesarias, RENOMBRAMOS LOS SUBFIJOS _x
base_bo = base_bo.drop(
    columns=[
        f"{SEGMENTO_TRANS}_y",
        f"{SUB_CANAL_TRANS}_y",
        f"{CANAL_TRANS}_y",
        SEGMENTO,
        CONCATENADAS[0],
        CONCATENADAS[1],
    ]
)

# Renombrar columnas necesarias.
base_bo = Renombrar_columnas_con_diccionario(
    base=base_bo,
    cols_to_rename={
        f"{CANAL_TRANS}_x": CANAL_TIP,
        f"{SUB_CANAL_TRANS}_x": SUB_CANAL_TIP,
        f"{SEGMENTO_TRANS}_x": SEGMENTO,
    },
)

# COLUMNA AGRUPACION_CLIENTES

# 1.)Traer la columna "Agrupacion Clientes" Driver Jerarquia cadenas para grandes cadenas
base_bo = pd_left_merge(
    base_left=base_bo,
    base_right=driver_agrup_clientes_gc,
    key=FORMATO_ORIG,
)

# COLUMNA FORMATO

# 1.) Traer la columna "Formato" Driver Jerarquia cadenas para grandes cadenas.
base_bo = pd_left_merge(
    base_left=base_bo,
    base_right=driver_formato_clientes_gc,
    key=FORMATO_ORIG,
)


# COLUMNA COD. JV , (Codigo de vendedores) y COLUMNA JEFE DE VENTAS.

# 1.) Concatenar las columnas COD_OV y FORMATO  para usarlas como clave
base_bo = concatenar_columnas(
    dataframe=base_bo, cols_elegidas=[FORMATO, COD_OV], nueva_columna=CLAVE
)

# 2.) Merge para traer el primer grupo de Cod JV y Nombre de JV
# El primer el grupo se consulta por la columna CLAVE, que acabamos de crear.
# Se trae de la columna COD_KAM  y NOMBRE_KAM.
base_bo = pd_left_merge(
    base_left=base_bo,
    base_right=driver_kam_formato_cod_jv,
    key=CLAVE,
)
# Eliminar columna ya innecesaria
base_bo = base_bo.drop([CLAVE], axis=1)

# 3.) Extraemos los indices que quedaron nulos, para ambas columnas.
# Columna COD_KAM Y NOMBRE_KAM fueron traidas por el merge.
indices_nulos_codjv = base_bo[base_bo[COD_KAM].isnull()].index
indices_nulos_nom_jv = base_bo[base_bo[NOMBRE_KAM].isnull()].index

# 4.) Merge para traer el segundo grupo de Cod JV
base_bo = pd_left_merge_two_keys(
    base_left=base_bo,
    base_right=driver_cods_jv_y_rv[[COD_LOC, COD_JV]].drop_duplicates(subset=COD_LOC),
    left_key=COD_CLI,
    right_key=COD_LOC,
)
# Se genera una duplicación. (La columna COD_LOC ya existia.)

# 5.) Realizamos el primer reemplazo de valores.
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=COD_KAM,
    columna_con_valores=COD_JV,
    indices=indices_nulos_codjv,
)
# Borramos la columna ya innecesaria.
base_bo = base_bo.drop([COD_JV], axis=1)

# 6.) Extraemos los indices que quedaron nulos luego del segundo remplazo.
indices_nulos_codjv_2 = base_bo[base_bo[COD_KAM].isnull()].index

# 7.) Merge para traer el tercer grupo de Cod JV y ***el segundo grupo de Cod RV ( Se usa posteriormente para reemplazar los codigos nulos del primer grupo como se ha hecho repetidamente en la base con diferente grupos de columnas para al final construir la  COLUMNA COD_RV***)
base_bo = pd_left_merge(
    base_left=base_bo,
    base_right=driver_cods_jv_y_rv[[COD_CLI, COD_JV, COD_RV]],
    key=COD_CLI,
)
# 8.) Realizar el segundo remplazo con el tercer grupo correspondientes para los JV.
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=COD_KAM,
    columna_con_valores=COD_JV,
    indices=indices_nulos_codjv_2,
)


# 9.) Merge para traer el segundo grupo Nombre JV.
base_bo = pd_left_merge_two_keys(
    base_left=base_bo,
    base_right=driver_nombre_figura_comercial.drop_duplicates(subset=COD_FC),
    left_key=COD_KAM,
    right_key=COD_FC,
)

# 10.) Extraemos los indices que quedaron nulos luego del primer merge.
indices_nulos_nomjv = base_bo[base_bo[NOMBRE_KAM].isnull()].index

# 11. Reemplazar los indices nulos restantes de Nombre JV (Columna NOMBRE_KAM los contiene por ahora.)
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=NOMBRE_KAM,
    columna_con_valores=NOMBRE_FIGURA_COMERCIAL,
    indices=indices_nulos_nomjv,
)
# Eliminamos las columnas ya innecesarias.
base_bo = base_bo.drop([COD_JV, NOMBRE_FIGURA_COMERCIAL, f"{COD_LOC}_y"], axis=1)
# Se borran porque ya esta no será la columna final porque son ya un grupo de datos innecesarios. Luego de todos los reemplazos en la columna COD_KAM , está es la columna que actuará como COD_JV y será renombrada en este momento, similarmente sucede con la columna NOMBRE_KAM.
base_bo = Renombrar_columnas_con_diccionario(
    base=base_bo, cols_to_rename={COD_KAM: COD_JV, NOMBRE_KAM: JEFE_VENTAS}
)

# COLUMNA COD_RV

# 1.)  Merge para traer el primer grupo de Códigos RV (Códigos de vendedor) de los drivers. (Recordarya nos tragimos el segunDo grupo.)
base_bo = pd_left_merge_two_keys(
    base_left=base_bo,
    base_right=(driver_cods_jv_y_rv[[COD_LOC, COD_RV]].drop_duplicates(subset=COD_LOC)),
    left_key=COD_CLI,
    right_key=COD_LOC,
)

# 2.) Tomar los indices nulos luego del primer merge.
indices_nulos_codrv = base_bo[base_bo[f"{COD_RV}_y"].isnull()].index

# 3.) Reemplazar los valores, de los indices nulos.
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=f"{COD_RV}_y",
    columna_con_valores=f"{COD_RV}_x",
    indices=indices_nulos_codrv,
)
# Eliminamos columnas innecesarias ( Que ya no se usan, que son grupos de datos ya utilizados de los drivers o que borramos para evitar luego duplicidad.)
base_bo = base_bo.drop([f"{COD_RV}_x", COD_LOC, COD_FC], axis=1)

# (Recordar como por orden f"{COD_RV}_y" se trajo *despues*,  pero corresponde al primer grupo, está sera la columna que renombraremos como Cod. RV, a la vez aprovechamos para renombrar la columna Cod. Loc original del df, que se duplico y habia quedado con subfijo _x.

base_bo = Renombrar_columnas_con_diccionario(
    base=base_bo, cols_to_rename={f"{COD_RV}_y": COD_RV, f"{COD_LOC}_x": COD_LOC}
)

# COLUMNA NOMBRE VENDEDOR.

# 1). Merge para traer los nombres de los vendedores especificos.
base_bo = pd_left_merge_two_keys(
    base_left=base_bo,
    base_right=driver_nombre_figura_comercial.drop_duplicates(subset=COD_FC),
    left_key=COD_RV,
    right_key=COD_FC,
)

# Renombrar columna vendedores
base_bo = Renombrar_columnas_con_diccionario(
    base=base_bo, cols_to_rename={NOMBRE_FIGURA_COMERCIAL: VENDEDOR}
)

# Borrar columnas innecesarias
base_bo = base_bo.drop([COD_FC], axis=1)

# COLUMNA CANAL EST y SUBCANAL EST.
# 1.) Merge para traer el primer gurpo de información del driver Estructura Ventas
base_bo = pd_left_merge(
    base_left=base_bo,
    base_right=driver_estr_de_vtas_cn_subcn_est[[COD_RV, CANAL_EST, SUB_CANAL_EST]],
    key=COD_RV,
)

# 2.) Reemplazar en la nueva columna CANAL_EST los indices donde las columnas la CANAL_TIP y SUB_CANAL_TIP  == "Grandes Cadenas", con la constante Cadenas.
base_bo = Reemplazar_columna_en_funcion_de_otra(
    df=base_bo,
    nom_columna_a_reemplazar=CANAL_EST,
    nom_columna_de_referencia=CANAL_TIP,
    mapeo=DICT_CN_TIP_GC,
)
base_bo = Reemplazar_columna_en_funcion_de_otra(
    df=base_bo,
    nom_columna_a_reemplazar=SUB_CANAL_EST,
    nom_columna_de_referencia=CANAL_TIP,
    mapeo=DICT_CN_TIP_GC,
)

# 3.) Tomar los indices aún nulos de la columna CANAL_EST
indices_nulos_cn_est = indices_nulos_codrv = base_bo[base_bo[CANAL_EST].isnull()].index

# 4.) Reemplazar los indices nulos.
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=CANAL_EST,
    columna_con_valores=CANAL_TIP,
    indices=indices_nulos_cn_est,
)

# 5) Reemplazar en la nueva columna SUB_CANAL_EST los indices, donde la columna Grandes Cadenas de la SUB_CANAL_TIP" == "Tiendas" y "Droguerias"
base_bo = Reemplazar_columna_en_funcion_de_otra(
    df=base_bo,
    nom_columna_a_reemplazar=SUB_CANAL_EST,
    nom_columna_de_referencia=SUB_CANAL_TIP,
    mapeo={"Droguerías": "Directa"},
)

# 6).  Tomar los indices aún nulos de la columna SUB_CANAL_EST
indices_nulos_cn_est = indices_nulos_codrv = base_bo[
    base_bo[SUB_CANAL_EST].isnull()
].index

# 7). Reemplazar los indices nulos.
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=SUB_CANAL_EST,
    columna_con_valores=SUB_CANAL_TIP,
    indices=indices_nulos_cn_est,
)

# COLUMNA_SOCIO.
# 1.)  Traer los dos grupos de socios, por las dos llaves.
# Primer grupo por llave "Cod. Cl"
# Segundo grupo por llave COD_CLIENTE_ORIG
base_bo = pd_left_merge(
    base_left=base_bo,
    base_right=driver_clientes_socios[[COD_CLI, SOCIOS]].drop_duplicates(),
    key=COD_CLI,
)

base_bo = pd_left_merge_two_keys(
    base_left=base_bo,
    base_right=driver_clientes_socios[[COD_CLI, SOCIOS]].drop_duplicates(),
    left_key=COD_CLI_ORIG,
    right_key=COD_CLI,
)


# 2.) Obtener los indices nulos del primer grupo.
indices_nulos_cn_est = base_bo[base_bo[f"{SOCIOS}_x"].isnull()].index

# 3.) Reemplazar los indices nulos para los socios del primer grupo
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=f"{SOCIOS}_x",
    columna_con_valores=f"{SOCIOS}_y",
    indices=indices_nulos_cn_est,
)

# Eliminar columnas, duplicadas, innecesarias, y renombrar las definitvas.
base_bo = base_bo.copy().drop([f"{COD_CLI}_y", f"{SOCIOS}_y"], axis=1)
base_bo = Renombrar_columnas_con_diccionario(
    base=base_bo, cols_to_rename={f"{COD_CLI}_x": COD_CLI, f"{SOCIOS}_x": SOCIOS}
)

# 4.) Agregar columna calculada Dcto Ant.
base_bo[DCTO_ANT] = base_bo["Dcto P.F. Ant."].astype(float) + base_bo[
    "Dcto Cond. Ant."
].astype(float)




# COLUMNA_CLIENTES_ESCALAS.
# 1.)  Traer los dos grupos de escalas, por las dos llaves.
# Primer grupo por llave f"{CANAL_TIP_AJUST}_x"
# Segundo grupo por llave COD_CLIENTE_ORIG
base_bo = pd_left_merge(
    base_left=base_bo, base_right=driver_escalas[[COD_CLI, ESCALAS]], key=COD_CLI
)

# Notar la duplicidad ahora de la columna COD_CLI. por lo tanto existen ahora los subfijos _x , _y.

base_bo = pd_left_merge_two_keys(
    base_left=base_bo,
    base_right=driver_escalas[[COD_CLI, ESCALAS]],
    left_key=COD_CLI_ORIG,
    right_key=COD_CLI,
)
# La segunda llave del COD_CLI  será exactamente la constante guardada aquí. Ya que no existe repeticion ahora por la generación de los subfijos anteriores.

# 2.) Obtener los indices nulos del primer grupo.
indices_nulos_cn_est = indices_nulos_codrv = base_bo[
    base_bo[f"{ESCALAS}_x"].isnull()
].index

# 3.) Reemplazar los indices nulos para los socios del primer grupo
base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=f"{ESCALAS}_x",
    columna_con_valores=f"{ESCALAS}_y",
    indices=indices_nulos_cn_est,
)

# Eliminar columnas, duplicadas, innecesarias, y renombrar las definitvas.
base_bo = base_bo.copy().drop([f"{COD_CLI}_y", f"{ESCALAS}_y"], axis=1)
base_bo = Renombrar_columnas_con_diccionario(
    base=base_bo, cols_to_rename={f"{COD_CLI}_x": COD_CLI, f"{ESCALAS}_x": ESCALAS}
)

base_bo[[SOCIOS, ESCALAS]] = base_bo[[SOCIOS, ESCALAS]].fillna(FIL_NO)

# base_bo.to_excel("Plantilla_bo.xlsx", index=False)

# COLUMNA PLAN DE NEGOCIOS.
# 1.)  Traer primer grupo plan de negocios. Cuando la columna "Canal Tip". == Autoservicios", traer los nif correspondientes.
base_cn_tip_autoservicios = base_bo[base_bo[CANAL_TIP].isin(["Autoservicios"])][
    [NIF, NIF_ORIG]
].drop_duplicates()


base_cn_tip_autoservicios = pd_left_merge(
    base_left=base_cn_tip_autoservicios,
    base_right=driver_clientes_plan_negocios.drop_duplicates(subset=NIF)[
        [NIF, TIPO_INVERSION]
    ],
    key=NIF,
)

base_cn_tip_autoservicios = pd_left_merge_two_keys(
    base_left=base_cn_tip_autoservicios,
    base_right=driver_clientes_plan_negocios.drop_duplicates(subset=NIF)[
        [NIF, TIPO_INVERSION]
    ],
    left_key=NIF_ORIG,
    right_key=NIF,
)

# Tomar los indices que quedaron nulos.
indices_nulos_plan_negocios = base_cn_tip_autoservicios[
    base_cn_tip_autoservicios[f"{TIPO_INVERSION}_x"].isnull()
].index


base_cn_tip_autoservicios = reemplazar_indices(
    df=base_cn_tip_autoservicios,
    columna_a_reemplazar=f"{TIPO_INVERSION}_x",
    columna_con_valores=f"{TIPO_INVERSION}_y",
    indices=indices_nulos_plan_negocios,
)

base_cn_tip_autoservicios[f"{TIPO_INVERSION}_x"] = base_cn_tip_autoservicios[
    f"{TIPO_INVERSION}_x"
].fillna(FIL_NO)

# Tomar ahora los tipos mayoritas.
base_sbcn_tip_mayoristas = base_bo[base_bo[SUB_CANAL_TIP].isin(["Mayoristas"])][
    [COD_CLI, COD_CLI_ORIG]
]

base_sbcn_tip_mayoristas = pd_left_merge(
    base_left=base_sbcn_tip_mayoristas,
    base_right=driver_clientes_plan_negocios.drop_duplicates(subset=COD_CLI)[
        [COD_CLI, TIPO_INVERSION]
    ],
    key=COD_CLI,
)

base_sbcn_tip_mayoristas = pd_left_merge_two_keys(
    base_left=base_sbcn_tip_mayoristas,
    base_right=driver_clientes_plan_negocios.drop_duplicates(subset=COD_CLI)[
        [COD_CLI, TIPO_INVERSION]
    ],
    left_key=COD_CLI_ORIG,
    right_key=COD_CLI,
)

indices_nulos_plan_negocios1 = base_sbcn_tip_mayoristas[
    base_sbcn_tip_mayoristas[f"{TIPO_INVERSION}_x"].isnull()
].index

base_sbcn_tip_mayoristas = reemplazar_indices(
    df=base_sbcn_tip_mayoristas,
    columna_a_reemplazar=f"{TIPO_INVERSION}_x",
    columna_con_valores=f"{TIPO_INVERSION}_y",
    indices=indices_nulos_plan_negocios1,
)
base_sbcn_tip_mayoristas[f"{TIPO_INVERSION}_x"] = base_sbcn_tip_mayoristas[
    f"{TIPO_INVERSION}_x"
].fillna(FIL_NO)

# Renombrar las columnas columnas. Donde separamos los valores para plan de negocio correspondientes a autoservicios, mayorista ( canal y sucanal respectivamente que cumplen esta condición. para nuevamente hacer un merge con la base principal.)
base_cn_tip_autoservicios = Renombrar_columnas_con_diccionario(
    base=base_cn_tip_autoservicios,
    cols_to_rename={f"{NIF}_x": NIF, f"{TIPO_INVERSION}_x": PLAN_DE_NEGOCIO},
)
base_sbcn_tip_mayoristas = Renombrar_columnas_con_diccionario(
    base=base_sbcn_tip_mayoristas,
    cols_to_rename={f"{COD_CLI}_x": COD_CLI, f"{TIPO_INVERSION}_x": PLAN_DE_NEGOCIO},
)

# Merge con la base principal
base_bo = pd_left_merge(
    base_left=base_bo,
    base_right=base_cn_tip_autoservicios[[NIF, PLAN_DE_NEGOCIO]].drop_duplicates(NIF),
    key=NIF,
)

base_bo = pd_left_merge(
    base_left=base_bo,
    base_right=base_sbcn_tip_mayoristas[[COD_CLI, PLAN_DE_NEGOCIO]].drop_duplicates(
        COD_CLI
    ),
    key=COD_CLI,
)

# Reemplazar los indices del segundo grupo en el primer grupo como se ha hecho multiples veces.
indices_nulos_plan_negocios2 = base_bo[base_bo[f"{PLAN_DE_NEGOCIO}_x"].isnull()].index

base_bo = reemplazar_indices(
    df=base_bo,
    columna_a_reemplazar=f"{PLAN_DE_NEGOCIO}_x",
    columna_con_valores=f"{PLAN_DE_NEGOCIO}_y",
    indices=indices_nulos_plan_negocios2,
)

base_bo = Renombrar_columnas_con_diccionario(
    base=base_bo, cols_to_rename={f"{PLAN_DE_NEGOCIO}_x": PLAN_DE_NEGOCIO}
)
base_bo[PLAN_DE_NEGOCIO] = base_bo[PLAN_DE_NEGOCIO].fillna(FIL_NO)


# Eliminar columnas adicionales innecesarias.
base_bo = base_bo.copy().drop([f"{PLAN_DE_NEGOCIO}_y"], axis=1)

# Filtrar Subcanal
#base_bo_filtrada = Filtrar_por_valores_excluidos(
#    df=base_bo,
#    columna="Sub Canal Tip.",
#    valores_excluir=["Tiendas", "Natural", "Consumo Local"],
#)

# Transformar las columnas necesarias en reales para las agrupaciones.
base_bo_filtrada = Cambiar_tipo_dato_multiples_columnas(
    base=base_bo, list_columns=COLS_SUM_PLANTILLA1_TD_VD, type_data=float
)

# Agrupaciones necesarias. ( base_agrup para la base TD_ Datos cliente)
base_bo_fil_select_td_cli = seleccionar_columnas(
    dataframe=base_bo_filtrada, cols_elegidas=COLS_AGRUP_PLANTILLA1_TD_D_CLI
)

cols_completas_tf_vtas_dtos = (
    COLS_AGRUP_PLANTILLA1_TD_VT_DTCOS + COLS_SUM_PLANTILLA1_TD_VD
)


base_bo_fil_select_td_vtas = seleccionar_columnas(
    dataframe=base_bo_filtrada, cols_elegidas=cols_completas_tf_vtas_dtos
)

for cada_col, cada_valor in config["cols_fillna_plantilla1"][
    "td_datos_cliente"
].items():
    base_bo_fil_select_td_cli.loc[:, cada_col] = base_bo_fil_select_td_cli.loc[
        :, cada_col
    ].fillna(cada_valor)

# Agrupaciones necesarias. ( base_agrup para la base TD_ Vtas Dctos)
for cada_col, cada_valor in config["cols_fillna_plantilla1"]["td_vtas_dtos"].items():
    base_bo_fil_select_td_vtas.loc[:, cada_col] = base_bo_fil_select_td_vtas.loc[
        :, cada_col
    ].fillna(cada_valor)

td_datos_cliente = Group_by_and_sum_cols(
    df=base_bo_fil_select_td_cli,
    group_col=COLS_AGRUP_PLANTILLA1_TD_D_CLI[0:-1],
    sum_col=[DCTO_ANT],
)


# Agrupaciones necesarias. ( base_agrup_td_vtas_dctos para la base TD_Vtas_Dctos.)
base_bo_fil_select_td_vtas.loc[:,COLS_AGRUP_PLANTILLA1_TD_VT_DTCOS]=base_bo_fil_select_td_vtas[COLS_AGRUP_PLANTILLA1_TD_VT_DTCOS].fillna("Sin asignar")

base_bo_fil_select_td_vtas.loc[:,COLS_SUM_PLANTILLA1_TD_VD]=base_bo_fil_select_td_vtas[COLS_SUM_PLANTILLA1_TD_VD].fillna(0)

base_agrup_td_vtas_dctos = base_bo_fil_select_td_vtas.groupby(
    COLS_AGRUP_PLANTILLA1_TD_VT_DTCOS,
    as_index=False,
)[COLS_SUM_PLANTILLA1_TD_VD].sum()


# base_agrup_td_vtas_dctos = Eliminar_acentos(base_agrup_td_vtas_dctos)

#Eliminar columna agrupadora.
td_datos_cliente_select = td_datos_cliente[COLS_AGRUP_PLANTILLA1_TD_D_CLI[0:-1]]
# Exportar resultados primera plantilla a Excel
exportar_a_excel(
    ruta_guardado=RESULTADOS_P1,
    df=td_datos_cliente_select,
    nom_hoja=config["Plantillas"]["Plantilla1"]["noms"][0],
)
exportar_a_excel(
    ruta_guardado=RESULTADOS_P1,
    df=base_agrup_td_vtas_dctos,
    nom_hoja=config["Plantillas"]["Plantilla1"]["noms"][1],
)
