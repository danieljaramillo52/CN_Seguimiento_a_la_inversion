import pandas as pd
from loguru import logger
from Transformation_functions import (
    Eliminar_primeros_n_caracteres,
    Eliminar_acentos,
    filtrar_indices_por_longitud,
    modificar_valores_filas2,
    Agregar_columna_constante,
    filtrar_por_valores,
    pd_left_merge,
    reemplazar_indices,
)
from Lectura_insumos import maestra_activos_seleccionada, maestra_inactivos_seleccionada

import config_constans as cc

TIPO_Z6 = cc.TIPO_Z6
TIPO_ZA = cc.TIPO_ZA
TIPO_Z1 = cc.TIPO_Z1
ACTIVOS = cc.ACTIVOS
INACTIVOS = cc.INACTIVOS
N_CLIENTE = cc.N_CLIENTE
ESTADO = cc.ESTADO
CLIENTE = cc.CLIENTE
NIF = cc.NIF
COD_LOC = cc.COD_LOC
COD_CLI = cc.COD_CLI
COD_JV = cc.COD_JV
COD_RV = cc.COD_RV
FUNC_IN = cc.FUNC_IN
N_IDENT_FI = cc.N_IDENT_FI


def clasificar_codigos(group):
    """
    Clasifica los códigos 'ZA', 'Z1' y 'Z6' en un grupo de datos.

    Args:
        group (pandas.DataFrame): Grupo de datos que contiene la columna con los códigos.

    Returns:
        str or None: Retorna 'ZA' si 'ZA' está presente en la columna,
        'Z1' si 'ZA' no está presente y 'Z1' está presente, 'Z6' si 'ZA' y 'Z1' no están presentes,
        None si ninguno de los códigos está presente.
    """
    try:
        if TIPO_ZA in group[FUNC_IN].values:
            return TIPO_ZA
        elif TIPO_Z1 in group[FUNC_IN].values:
            return TIPO_Z1
        elif TIPO_Z6 in group[FUNC_IN].values:
            return TIPO_Z6
        else:
            return None
    except Exception as e:
        logger.error(f"Error al clasificar códigos: {str(e)}")
        return None


# Limpieza de maestras.
# Agregar columna activos e inactivos.
maestra_activos_seleccionada = Agregar_columna_constante(
    maestra_activos_seleccionada, ESTADO, ACTIVOS
)
maestra_inactivos_seleccionada = Agregar_columna_constante(
    maestra_inactivos_seleccionada, ESTADO, INACTIVOS
)
# Concatenar las maestras de clientes.
maestra_concatenada = pd.concat(
    [maestra_activos_seleccionada, maestra_inactivos_seleccionada], axis=0
)
# Eliminar primeros dos caracteres (0s) de la columna N_cliente.
#maestra_concatenada = Eliminar_primeros_n_caracteres(
#    df=maestra_concatenada.copy(), columna=N_CLIENTE, n=2
#)
# Filtrar indices clientes con inconsistencia en la longitud de sus codigos de cliente.
#indices_a_filtrar = filtrar_indices_por_longitud(
#    df=maestra_concatenada.copy(), columna=N_CLIENTE, n=8
#)
# Reemplazar con código localizador los clientes con la longitud no deseada.
# La funcion "modificar valores filas", toma una columna base en la cual se harán los reemplazos por los valores de una columna diferente, solo en los indices que pasemos como parámetro a la función.
#maestra_filtrada = modificar_valores_filas2(
#    df=maestra_concatenada.copy(),
#    indices=indices_a_filtrar,
#    columna=N_CLIENTE,
#    columna_valores=COD_LOC,
#)

# Eliminar_duplicados de la maestra_filtrada.
maestra_filtrada_mod = maestra_concatenada.drop_duplicates(subset=[N_CLIENTE, FUNC_IN])

clientes_medibles = maestra_filtrada_mod[N_CLIENTE]

# Seleccionar columnas N_CLIENTE y FUNC_IN
maestra_filtrada_insumo = maestra_filtrada_mod[[N_CLIENTE, FUNC_IN, "Num person"]]


# Filtra las filas donde FUNC_IN está en TIPO_Z6
base_cods_Z6 = filtrar_por_valores(maestra_filtrada_insumo, FUNC_IN, cc.TIPO_Z6)

# Renombrar la columna "Funcion in" como JF
base_cods_Z6_rename = base_cods_Z6.rename(columns={"Num person": COD_JV})
# Seleccionar solo las columnas necesarias.
base_cods_Z6_seleccionada = base_cods_Z6_rename[[N_CLIENTE, COD_JV]]


# Aplicar la función a lo largo de las filas agrupadas por 'ID'
base_cods_ZA_Z1 = maestra_filtrada_insumo.groupby(N_CLIENTE).apply(clasificar_codigos)

# Resetear los índices para obtener un DataFrame plano.
base_cods_ZA_Z1_reindex = base_cods_ZA_Z1.reset_index(name=FUNC_IN)

# Obtener los códigos segun el tipo.
base_cods_ZA_Z1_merge = pd.merge(
    base_cods_ZA_Z1_reindex,
    maestra_filtrada_insumo,
    on=[N_CLIENTE, FUNC_IN],
    how="left",
)
base_cods_ZA_Z1_rename = base_cods_ZA_Z1_merge.rename(columns={"Num person": COD_RV})

# Seleccionar columnas necesarias.
base_cods_ZA_Z1_seleccionada = base_cods_ZA_Z1_rename[[N_CLIENTE, COD_RV]]

# Merge entre JF y RV (Jefes de ventas y Vendedores)
vendedores_y_jf = pd_left_merge(
    base_left=base_cods_ZA_Z1_seleccionada,
    base_right=base_cods_Z6_seleccionada,
    key=N_CLIENTE,
)

# Merge con la base maestra inicial
maestra_final = pd_left_merge(
    base_left=maestra_filtrada_mod, base_right=vendedores_y_jf, key=N_CLIENTE
)

### DRIVER Códigos Jefes de Venta & Vendedores
### Recordar en este contexto un driver se refiere a una tabla, que puede ya estar en un documento excel. Un dataframe o un subjconjunto de columnas del mismo que será exportado a excel.
driver_cod_jv_ven = maestra_final[[N_CLIENTE, COD_LOC, COD_JV, COD_RV]]

indices_nulos_cod_jv = driver_cod_jv_ven[
    driver_cod_jv_ven[COD_JV].isnull()
].index

driver_cod_jv_ven_mod = reemplazar_indices(
    df=driver_cod_jv_ven,
    columna_a_reemplazar=COD_JV,
    columna_con_valores=COD_RV,
    indices=indices_nulos_cod_jv,
)
driver_cod_jv_ven_rename = driver_cod_jv_ven_mod.rename(columns={N_CLIENTE: COD_CLI})


### Modificación códgios localizadores por COD_CLI #OJOOO#
driver_cod_jv_ven_rename.loc[:,COD_LOC] = driver_cod_jv_ven_rename[COD_CLI]
## Fin modificación N°1

driver_cod_jv_ven_filtrada_1 = driver_cod_jv_ven_rename.drop_duplicates(subset=[COD_CLI])
driver_cod_jv_ven_filtrada_2 = Eliminar_acentos(
    driver_cod_jv_ven_filtrada_1)
### Reemplazar los Jefes de venta vacios.

###DRIVER Códigos Cliente Nif-de-Maestra.
maestra_cliente_NifMaestra = maestra_filtrada_mod[[N_CLIENTE, N_IDENT_FI]]
maestra_cliente_NifMaestra_rename = maestra_cliente_NifMaestra.rename(
    columns={N_CLIENTE: CLIENTE, N_IDENT_FI: NIF}
)
maestra_cliente_NifMaestra_filtrada = Eliminar_acentos(
    maestra_cliente_NifMaestra_rename.drop_duplicates(subset=CLIENTE)
)
